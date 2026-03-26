# Bottleneck Analysis — Ingestion Workflow `pdgs-omnipass-ingestion-2884-7b7cb`

**Cluster:** dev
**Namespace:** datalake
**Nodes involved:** devmkpl-n04, devmkpl-n07
**PVC (working dir):** `pvc-c97f5304-af24-474c-84f4-a8cb08431cd5`
**Storage class:** Longhorn (RWX via NFS share-manager)
**Total workflow duration:** ~5m41s (341s)

---

## 1. Workflow Step Timeline

Timestamps derivati dagli eventi Kubernetes. T=0 = avvio workflow.

```
T+0s    PVC provisioned (2s — trascurabile)

T+11s   resolve-config    n07   init=2s    [nessun PVC]
T+21s   prepare           n07   init=14s   [vol_attach=10s]
T+33s   cwl-prepare       n07   init=9s    [nessun PVC — 9s unexplained *]
T+46s   stage-in          n04   init=29s   [vol_attach=27s]  ← primo mount n04
T+69s   calrissian-tmpl   n04   init=2s    [nessun PVC]
T+96s   step-1-pod        n07   init=7s    [vol_attach=2s]   ← mount n07 già caldo
        [CWL omnipass in esecuzione ~90s]
T+180s  get-results       n04   init=27s   [vol_attach=24s]  ← rimount n04
T+180s  stage-out         n04   init=27s   [vol_attach=~25s] ← rimount n04 §
T+225s  post-results      n04   init=38s   [vol_attach=37s, 1x FailedAttachVolume]
T+271s  message-passthrough n07 init=27s   [vol_attach=23s]  ← rimount n07
T+271s  resolve-config(2) n07   init=3s    [nessun PVC]
T+303s  main(send-msg)    n07   init=3s    [nessun PVC, img_pull=237ms †]
T+341s  WORKFLOW COMPLETED
```

`*` cwl-prepare non monta il PVC ma impiega 9s di init: overhead K8s (secret injection, emptyDir, configurazione namespace).
`§` stage-out non ha emesso eventi SuccessfulAttachVolume pur attendendo ~25s: era in coda dietro get-results sullo stesso nodo (n04) per lo stesso NodeStageVolume.
`†` L'immagine `container-send-message` non è in cache (pull 237ms, 172MB).

---

## 2. Analisi volume attachment per step

| Step | Nodo | vol_attach | FailedAttach | image_pull | init_total | vol% di init |
|------|------|-----------|--------------|------------|-----------|--------------|
| prepare | n07 | 10s | 0 | 94ms | 14s | 71% |
| stage-in | n04 | 27s | 0 | 66ms | 29s | 93% |
| step-1-pod | n07 | 2s | 0 | 0ms (cached) | 7s | 29% |
| get-results | n04 | 24s | 0 | 125ms | 27s | 89% |
| stage-out | n04 | ~25s (§) | 0 | 90ms | 27s | ~93% |
| post-results | n04 | 37s | 1 | 86ms | 38s | 97% |
| message-passthrough | n07 | 23s | 0 | 243ms | 27s | 85% |

**Tempo totale sprecato in volume attachment per run: ~148s (43% del workflow)**

Le immagini applicative sono tutte in cache (pull < 150ms): non contribuiscono al problema.

---

## 3. Causa radice

### 3.1 Il pattern dei remount Longhorn

Con Longhorn RWX ogni nodo monta la share NFS tramite il CSI driver:
- `NodeStageVolume`: monta la share NFS sul nodo host (operazione pesante, 20-37s)
- `NodePublishVolume`: bind-mount nel pod (< 1s)
- `NodeUnstageVolume`: smonta la share quando l'**ultimo pod su quel nodo** che usa il volume termina

Poiché gli step Argo sono pod **sequenziali**, dopo che ciascun pod termina Longhorn chiama `NodeUnstageVolume`. Il pod successivo trova la share smontata e deve aspettare un nuovo `NodeStageVolume`.

```
prepare avvia   → NodeStageVolume n07 (10s)
prepare termina → NodeUnstageVolume n07

stage-in avvia  → NodeStageVolume n04 (27s)
stage-in termina → NodeUnstageVolume n04

get-results avvia → NodeStageVolume n04 (24s)  ← rimonta
  stage-out avvia → reusa mount di get-results (0s aggiuntivi, ma aspetta get-results)
get-results + stage-out terminano → NodeUnstageVolume n04

post-results avvia → NodeStageVolume n04 (37s) ← rimonta di nuovo, con failure
```

### 3.2 Perché il rimount è lento (20-37s)?

Il CSI driver Longhorn deve:
1. Verificare che il volume sia "healthy" (repliche in sync, share-manager attivo)
2. Il share-manager pod (NFS server) potrebbe essere occupato a gestire la disconnessione precedente
3. `FailedAttachVolume` su post-results indica che il volume era in stato "not ready for workloads" — Longhorn era ancora in transizione da un detach precedente

### 3.3 Cosa funziona bene

- **step-1-pod** (il tool CWL effettivo): attach in soli 2s perché cwl-prepare mantiene il volume montato su n07 durante tutta l'esecuzione Calrissian. Calrissian gestisce correttamente il suo working directory.
- **PVC provisioning iniziale**: 2s, trascurabile.
- **Image cache**: tutte le immagini operative sono in cache. Unica eccezione: `container-send-message` (237ms, 172MB) — step non critico (onExit).

---

## 4. Discrepanze tra eventi reali e calcoli dell'observer

| Step | Metrica | Valore reale (eventi K8s) | Valore observer | Discrepanza |
|------|---------|--------------------------|-----------------|-------------|
| post-results | volume_attach_sec | 37s (scheduled→success) | **17s** (era failure→success) | -20s (fix applicato) |
| stage-out | volume_attach_sec | ~25s (attesa NodeStageVolume) | **None** (nessun evento emesso) | non misurabile da eventi |
| stage-out | CASE F causa | Longhorn remount | **non identificata** (vol_attach=None) | gap diagnostico |
| cwl-prepare | init 9s | K8s overhead (no PVC) | corretto ma causa ignota | secondario |

### Fix applicato

Il calcolo `volume_attach_sec` è stato corretto: la reference è ora sempre `pod_scheduled_at → SuccessfulAttachVolume` (tempo totale realmente atteso dal pod), invece di `first_failure → SuccessfulAttachVolume` che escludeva il periodo silenzioso pre-failure.

### Gap residuo: stage-out

Longhorn non emette `SuccessfulAttachVolume` quando due pod sullo stesso nodo competono per lo stesso `NodeStageVolume` contemporaneamente: solo il primo pod riceve l'evento, il secondo aspetta silenziosamente. L'observer non può misurare questo caso da eventi K8s.

**Workaround diagnostico**: se `init_duration_sec > 15s` ma `volume_attach_sec = None` e `image_pull_sec < 1s`, la causa probabile è la contesa su NodeStageVolume (step concorrenti sullo stesso nodo).

---

## 5. Raccomandazioni

### R1 — Cambiare storage class (impatto: alto, effort: medio)

**Problema risolto**: elimina interamente il NodeStageVolume overhead.

Longhorn RWX implementa NFS tramite un pod share-manager per-volume. Ogni mount/unmount del nodo costa 20-37s. Un NFS provisioner tradizionale (es. `nfs-subdir-external-provisioner`) crea directory su un server NFS preesistente: il mount è istantaneo (< 1s).

```bash
# Verifica storage class disponibili nel cluster
kubectl get storageclass
```

Se esiste già un provisioner NFS, la modifica è un singolo campo nel template Calrissian:
```yaml
storageClassName: nfs-client   # invece di longhorn
accessModes: [ReadWriteMany]
```

**Risparmio atteso**: ~148s per run → ~40s per run (solo il primo attach, poi reuse).

---

### R2 — "Keeper" pod per mantenere il mount aperto (impatto: alto, effort: basso)

**Problema risolto**: elimina i remount tra step sequenziali senza cambiare storage class.

Aggiungere uno step Argo leggero che monta il PVC e gira **per tutta la durata del workflow**, impedendo `NodeUnstageVolume` tra uno step e l'altro.

```yaml
# Step aggiuntivo nel workflow Argo (sidecar-like, usa suspend o sleep)
- name: pvc-keeper
  template: pvc-keeper-tmpl
  # Lancia in parallelo con il primo step che usa il PVC
  # Termina solo dopo l'ultimo step che usa il PVC
```

Alternativa: eseguire tutti gli step PVC-dipendenti come container di un **singolo pod** (template `steps` → `dag` con un pod multi-container). Meno invasivo architetturalmente.

**Risparmio atteso**: ~148s → ~10s per run (solo il primissimo mount).

---

### R3 — Node affinity + Longhorn dataLocality (impatto: medio, effort: basso)

**Problema risolto**: riduce il numero di NodeStageVolume e minimizza la latenza di mount.

Forzare tutti gli step PVC-dipendenti sullo stesso nodo (affinity `requiredDuringSchedulingIgnoredDuringExecution`) riduce le sequenze di mount/unmount da 2 nodi a 1. Con `dataLocality: strict-local` su Longhorn il mount è locale (loopback NFS), più veloce di un mount remoto.

Nota: riduce ma **non elimina** il rimount tra step sequenziali.

---

### R4 — Aggiungere coresMin/ramMin al CWL (impatto: basso, effort: minimo)

**Problema risolto**: K8s scheduling più affidabile, evita over-scheduling.

Il `appPackage` attuale dichiara solo `coresMax`/`ramMax`. Aggiungere i valori minimi:

```json
"ResourceRequirement": {
  "coresMin": 1,
  "coresMax": 2,
  "ramMin": 2048,
  "ramMax": 3192
}
```

Questo garantisce `resources.requests` definiti sui pod K8s: lo scheduler può piazzare i pod su nodi con risorse effettivamente disponibili.

---

### R5 — Pre-caching immagine container-send-message (impatto: minimo, effort: minimo)

L'immagine `container-send-message` (172MB) non è in cache sui nodi e impiega 237ms al pull. Aggiungere al DaemonSet image-prepuller se esiste, altrimenti secondario rispetto agli altri fix.

---

## 6. Priorità interventi

| Priorità | Intervento | Risparmio stimato | Effort |
|----------|-----------|-------------------|--------|
| 1 | R1 — Cambiare storage class | ~108s/run (73% riduzione overhead) | Medio |
| 2 | R2 — Keeper pod | ~138s/run (93% riduzione overhead) | Basso |
| 3 | R3 — Node affinity + dataLocality | ~30-50s/run (parziale) | Basso |
| 4 | R4 — CWL coresMin/ramMin | Indiretto (scheduling) | Minimo |
| 5 | R5 — Pre-cache send-message | 237ms/run | Minimo |

**Raccomandazione**: implementare R2 (keeper pod) come fix immediato a basso sforzo, e pianificare R1 (cambio storage class) per eliminare la causa strutturale.

---

## 7. Metriche target post-fix

Con R1 + R2 implementati:

| Metrica | Attuale | Target |
|---------|---------|--------|
| init_duration_sec (prepare) | 14s | < 3s |
| init_duration_sec (stage-in) | 29s | < 3s |
| init_duration_sec (get-results) | 27s | < 3s |
| init_duration_sec (post-results) | 38s | < 3s |
| Overhead volume attachment totale/run | ~148s | < 10s |
| Workflow duration totale | ~341s | ~193s (-43%) |
