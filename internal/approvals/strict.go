package approvals

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// shape is the exact keys an object may carry, and the shape of each key
// whose value is itself an object.
type shape struct {
	keys    map[string]bool
	objects map[string]*shape
}

func keys(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

var (
	askerShape    = &shape{keys: keys("ageRecipient", "name")}
	approverShape = &shape{keys: keys("label", "signKey")}
	requestShape  = &shape{
		keys:    keys("actors", "asker", "choices", "comments", "createdAt", "expiresAt", "requestId", "text", "v"),
		objects: map[string]*shape{"asker": askerShape},
	}
	statementShape = &shape{
		keys:    keys("answer", "approver", "asker", "expiresAt", "requestHash", "requestId", "signedAt", "text", "v"),
		objects: map[string]*shape{"asker": askerShape, "approver": approverShape},
	}
	signedShape = &shape{keys: keys("signature", "statement")}
	proofShape  = &shape{keys: keys("request", "signature", "statement")}
)

// strictUnmarshal decodes data into v only if every object in it carries
// exactly-cased keys of its shape, none twice, and nothing else. A phone
// reads JSON case-sensitively and takes the last of a duplicate key; Go's
// decoder matches keys case-insensitively. Without this, one signed byte
// string could say one thing to the person and another to this client.
func strictUnmarshal(data []byte, s *shape, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := walkValue(dec, s, "value"); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return fmt.Errorf("data after the JSON value")
	}
	return json.Unmarshal(data, v)
}

func walkValue(dec *json.Decoder, s *shape, where string) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch tok {
	case json.Delim('{'):
		if s == nil {
			return fmt.Errorf("%s: an object where none belongs", where)
		}
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			k := kt.(string)
			if seen[k] {
				return fmt.Errorf("%s: key %q twice", where, k)
			}
			seen[k] = true
			if !s.keys[k] {
				return fmt.Errorf("%s: unexpected key %q", where, k)
			}
			if err := walkValue(dec, s.objects[k], k); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	case json.Delim('['):
		for dec.More() {
			if err := walkValue(dec, nil, where); err != nil {
				return err
			}
		}
		_, err := dec.Token()
		return err
	}
	return nil
}
