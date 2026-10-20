{{- define "astryon-gateway.name" -}}astryon-gateway{{- end }}
{{- define "astryon-gateway.fullname" -}}{{ .Release.Name }}-{{ include "astryon-gateway.name" . }}{{- end }}
{{- define "astryon-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}{{ default (include "astryon-gateway.fullname" .) .Values.serviceAccount.name }}{{ else }}{{ required "serviceAccount.name is required when create=false" .Values.serviceAccount.name }}{{ end }}
{{- end }}
