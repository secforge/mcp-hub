package wsserver

import "encoding/json"

func jsonUnmarshal(raw []byte, v any) error {
	return json.Unmarshal(raw, v)
}
