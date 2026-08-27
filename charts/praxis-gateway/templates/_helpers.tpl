{{/*
Chart name, truncated to 63 characters.
*/}}
{{- define "praxis-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name.
*/}}
{{- define "praxis-gateway.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label value: name-version.
*/}}
{{- define "praxis-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Standard Kubernetes labels applied to every resource.
*/}}
{{- define "praxis-gateway.labels" -}}
helm.sh/chart: {{ include "praxis-gateway.chart" . }}
{{ include "praxis-gateway.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels used by Deployment matchLabels and Service selectors.
*/}}
{{- define "praxis-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "praxis-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Container image reference.
Defaults the tag to the chart appVersion when no digest or tag is set.
*/}}
{{- define "praxis-gateway.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}
{{- end }}

{{/*
Overlay-sync image reference.
Defaults the tag to the chart appVersion when empty.
*/}}
{{- define "praxis-gateway.overlaySyncImage" -}}
{{- printf "%s:%s" .Values.overlay.sidecar.image.repository (default .Chart.AppVersion .Values.overlay.sidecar.image.tag) }}
{{- end }}

{{/*
Validate image digest format when provided.
*/}}
{{- define "praxis-gateway.validateDigest" -}}
{{- if and .Values.image.digest (not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest)) }}
{{- fail "image.digest must be in the form sha256:<64 hex characters>" }}
{{- end }}
{{- end }}

{{/*
Validate required config ConfigMap name.
*/}}
{{- define "praxis-gateway.validateConfig" -}}
{{- if and (not .Values.config.existingConfigMap) (not .Values.config.create) }}
{{- fail "config.existingConfigMap or config.create=true is required" }}
{{- end }}
{{- if and .Values.config.create (not .Values.config.data) }}
{{- fail "config.data is required when config.create=true" }}
{{- end }}
{{- end }}

{{- define "praxis-gateway.configMapName" -}}
{{- if .Values.config.create }}{{ include "praxis-gateway.fullname" . }}-config{{ else }}{{ .Values.config.existingConfigMap }}{{ end }}
{{- end }}

{{/*
Validate enabled mounts have a non-empty resource name.
*/}}
{{- define "praxis-gateway.validateMounts" -}}
{{- if and .Values.overlay.enabled (not .Values.overlay.existingConfigMap) }}
{{- fail "overlay.existingConfigMap is required when overlay.enabled is true" }}
{{- end }}
{{- if and .Values.overlay.enabled .Values.overlay.sidecar.enabled (not .Values.overlay.sidecar.expectedNetwork) }}
{{- fail "overlay.sidecar.expectedNetwork is required when overlay sidecar is enabled" }}
{{- end }}
{{- if and .Values.overlay.enabled .Values.overlay.sidecar.enabled (not .Values.overlay.sidecar.expectedLocalSite) }}
{{- fail "overlay.sidecar.expectedLocalSite is required when overlay sidecar is enabled" }}
{{- end }}
{{- if and .Values.tls.enabled (not .Values.tls.existingSecret) }}
{{- fail "tls.existingSecret is required when tls.enabled is true" }}
{{- end }}
{{- end }}
