{{- define "astryon-service.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "astryon-service.fullname" -}}
{{- if .Values.fullnameOverride -}}{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" -}}{{- end -}}
{{- end -}}
{{- define "astryon-service.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{ include "astryon-service.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: astryon
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}
{{- define "astryon-service.selectorLabels" -}}
app.kubernetes.io/name: {{ include "astryon-service.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "astryon-service.sa" -}}
{{- if .Values.serviceAccount.create -}}{{ include "astryon-service.fullname" . }}{{- else -}}default{{- end -}}
{{- end -}}
