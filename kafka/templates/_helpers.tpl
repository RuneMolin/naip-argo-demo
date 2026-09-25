{{- define "kafka.labels" -}}
strimzi.io/cluster: {{ .Values.kafka.clusterName }}
app.kubernetes.io/instance: {{ .Values.kafka.clusterName }}
{{- end }}
