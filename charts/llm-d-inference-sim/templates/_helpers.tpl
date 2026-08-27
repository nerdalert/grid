{{- define "llm-d-inference-sim.name" -}}{{ default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}{{- end }}
{{- define "llm-d-inference-sim.fullname" -}}{{ if .Values.fullnameOverride }}{{ .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{ else }}{{ include "llm-d-inference-sim.name" . }}{{ end }}{{- end }}
{{- define "llm-d-inference-sim.image" -}}{{ if .Values.image.digest }}{{ .Values.image.repository }}@{{ .Values.image.digest }}{{ else }}{{ .Values.image.repository }}:{{ .Values.image.tag }}{{ end }}{{- end }}
{{- define "llm-d-inference-sim.labels" -}}
app.kubernetes.io/name: llm-d-inference-sim
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
