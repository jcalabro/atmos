package crypto

import "testing"

func BenchmarkP256_Sign(b *testing.B) {
	priv, _ := GenerateP256()
	data := []byte("benchmark signing data for atproto")
	b.ResetTimer()
	for b.Loop() {
		_, _ = priv.HashAndSign(data)
	}
}

func BenchmarkP256_Verify(b *testing.B) {
	priv, _ := GenerateP256()
	data := []byte("benchmark signing data for atproto")
	sig, _ := priv.HashAndSign(data)
	pub := priv.PublicKey()
	b.ResetTimer()
	for b.Loop() {
		_ = pub.HashAndVerify(data, sig)
	}
}

func BenchmarkK256_Sign(b *testing.B) {
	priv, _ := GenerateK256()
	data := []byte("benchmark signing data for atproto")
	b.ResetTimer()
	for b.Loop() {
		_, _ = priv.HashAndSign(data)
	}
}

func BenchmarkK256_Verify(b *testing.B) {
	priv, _ := GenerateK256()
	data := []byte("benchmark signing data for atproto")
	sig, _ := priv.HashAndSign(data)
	pub := priv.PublicKey()
	b.ResetTimer()
	for b.Loop() {
		_ = pub.HashAndVerify(data, sig)
	}
}

func BenchmarkParseDIDKey_P256(b *testing.B) {
	priv, _ := GenerateP256()
	didKey := priv.PublicKey().DIDKey()
	b.ResetTimer()
	for b.Loop() {
		_, _ = ParsePublicDIDKey(didKey)
	}
}

func BenchmarkParseDIDKey_K256(b *testing.B) {
	priv, _ := GenerateK256()
	didKey := priv.PublicKey().DIDKey()
	b.ResetTimer()
	for b.Loop() {
		_, _ = ParsePublicDIDKey(didKey)
	}
}
