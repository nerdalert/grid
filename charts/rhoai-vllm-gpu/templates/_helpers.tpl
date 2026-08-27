{{- define "rhoai-vllm-gpu.fullname" -}}
{{- printf "%s" .Release.Name | trunc 40 | trimSuffix "-" -}}
{{- end -}}
