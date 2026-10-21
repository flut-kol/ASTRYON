{{- define "astryon.fullname" -}}{{ printf "%s-%s" .root.Release.Name .key | trunc 63 | trimSuffix "-" }}{{- end }}

{{- define "astryon.selector" -}}
app.kubernetes.io/name: astryon
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .key }}
{{- end }}

{{- define "astryon.labels" -}}
{{ include "astryon.selector" . }}
app.kubernetes.io/part-of: astryon
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end }}

{{/* Merged plain env as YAML: AAS + kafka + common + service env (service wins). Fails on dev-only/insecure keys and missing required keys. */}}
{{- define "astryon.env" -}}
{{- $root := .root }}{{- $s := .svc }}
{{- $e := dict }}
{{- if $s.aas }}
{{- $p := default "" $s.aasPrefix }}
{{- range $k, $v := dict "BASE_URL" $root.Values.aas.baseUrl "ISSUER" $root.Values.aas.issuer "AUDIENCE" $root.Values.aas.audience "ENV" $root.Values.aas.env "ALLOWED_AZP" $root.Values.aas.allowedAzp "TOKEN_TYPE" $root.Values.aas.tokenType }}
{{- if $v }}{{ $_ := set $e (printf "%sAAS_%s" $p $k) $v }}{{ end }}
{{- end }}
{{- end }}
{{- if $s.aasBaseOnly }}{{ if $root.Values.aas.baseUrl }}{{ $_ := set $e "AAS_BASE_URL" $root.Values.aas.baseUrl }}{{ end }}{{ end }}
{{- if $s.kafka }}
{{- if $root.Values.kafka.brokers }}{{ $_ := set $e "ASTRYON_KAFKA_BROKERS" $root.Values.kafka.brokers }}{{ end }}
{{- if $root.Values.kafka.tls }}{{ $_ := set $e "ASTRYON_KAFKA_TLS" "1" }}{{ end }}
{{- if $root.Values.kafka.caSecret }}{{ $_ := set $e "ASTRYON_KAFKA_TLS_CA_FILE" "/var/run/secrets/astryon/kafka-ca/ca.crt" }}{{ end }}
{{- if $root.Values.kafka.saslMechanism }}{{ $_ := set $e "ASTRYON_KAFKA_SASL_MECHANISM" $root.Values.kafka.saslMechanism }}{{ end }}
{{- end }}
{{- range $k := default (list) $s.commonEnv }}
{{- $v := get $root.Values.common $k }}{{ if $v }}{{ $_ := set $e $k $v }}{{ end }}
{{- end }}
{{- range $k, $v := default (dict) $s.env }}{{ $_ := set $e $k $v }}{{ end }}
{{- range $k, $v := $e }}
{{- if or (contains "_DEV_" $k) (eq $k "BFF_COOKIE_INSECURE") (eq $k "BFF_CLIENT_SECRET") }}{{ fail (printf "service %s: env %s is dev-only/insecure and must not be set in a deployment" $.key $k) }}{{ end }}
{{- end }}
{{- range $k := default (list) $s.requiredEnv }}
{{- if not (get $e $k) }}{{ fail (printf "service %s: env %s is required (set it in the environment values file)" $.key $k) }}{{ end }}
{{- end }}
{{- toYaml $e }}
{{- end }}
