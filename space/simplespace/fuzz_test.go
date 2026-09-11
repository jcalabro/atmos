package simplespace

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
)

func FuzzDecodeCreatePolicy(f *testing.F) {
	f.Add([]byte(`{"type":"com.example.board","readPolicy":{"$type":"com.atproto.simplespace.defs#publicPolicy"},"writePolicy":{"$type":"com.atproto.simplespace.defs#memberListPolicy"},"appAccess":{"$type":"com.atproto.simplespace.defs#open"}}`))
	f.Add([]byte(`{"type":"com.example.board","readPolicy":{"$type":"evil.example#public"},"writePolicy":{"$type":"evil.example#public"},"appAccess":{"$type":"evil.example#open"}}`))
	f.Add([]byte(`null`))
	space := atmos.SpaceRef("at://did:plc:abcdefghijklmnopqrstuvwx/space/com.example.board/main")
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		var input comatproto.SimplespaceCreateSpace_Input
		if err := json.Unmarshal(data, &input); err != nil {
			return
		}
		config, err := DecodeCreate(space, &input)
		if err == nil {
			if err := config.Validate(); err != nil {
				t.Fatalf("DecodeCreate returned invalid config: %v", err)
			}
		}
	})
}
