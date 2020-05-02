Tables
======

{{ range .Data.Tables }}
{{ snakeToTitle .Name }}
-----------

{{ .Description }}

| Name | Type | Description |
|------|------|-------------|
{{ range .Columns -}}
| {{ .Name }} | {{ if .CustomType }}[{{.DataType}}](#{{anchor .DataType}}){{ else }}{{.DataType}}{{ end }} | {{ mdescape .Description}} |
{{ end }}
{{ end }}


Enums
=====

{{ range .Data.Enums }}
{{ snakeToTitle .Name }}
-------------------------
{{ if .Description }}
{{ .Description }}
{{ else }}
{{ range .Values -}}
- `{{ . }}`
{{ end }}
{{- end }}
{{ end }}
