package common

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"encoding/binary"
  	"fmt"
)

// SOURCE: https://gist.github.com/miguelmota/3ea9286bd1d3c2a985b67cac4ba2130a

// GenerateKeyPair generates a new key pair
func GenerateKeyPair(bits int) (*rsa.PrivateKey, *rsa.PublicKey) {
	privkey, err := rsa.GenerateKey(rand.Reader, 256)
	if err != nil {
		fmt.Println(err)
	}
	return privkey, &privkey.PublicKey
}

// PrivateKeyToBytes private key to bytes
func PrivateKeyToBytes(priv *rsa.PrivateKey) []byte {
	privBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(priv),
		},
	)

	return privBytes
}

// PublicKeyToBytes public key to bytes
func PublicKeyToBytes(pub *rsa.PublicKey) []byte {
	pubASN1, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		fmt.Println(err)
	}

	pubBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: pubASN1,
	})

	return pubBytes
}

// BytesToPrivateKey bytes to private key
func BytesToPrivateKey(priv []byte) *rsa.PrivateKey {
	block, _ := pem.Decode(priv)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	var err error
	if enc {
		fmt.Println("is encrypted pem block")
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			fmt.Println(err)
		}
	}
	key, err := x509.ParsePKCS1PrivateKey(b)
	if err != nil {
		fmt.Println(err)
	}
	return key
}

// BytesToPublicKey bytes to public key
func BytesToPublicKey(pub []byte) *rsa.PublicKey {
	block, _ := pem.Decode(pub)
	enc := x509.IsEncryptedPEMBlock(block)
	b := block.Bytes
	var err error
	if enc {
		fmt.Println("is encrypted pem block")
		b, err = x509.DecryptPEMBlock(block, nil)
		if err != nil {
			fmt.Println(err)
		}
	}
	ifc, err := x509.ParsePKIXPublicKey(b)
	if err != nil {
		fmt.Println(err)
	}
	key, ok := ifc.(*rsa.PublicKey)
	if !ok {
		fmt.Println("not ok")
	}
	return key
}

// EncryptWithPublicKey encrypts data with public key
func EncryptWithPublicKey(msg []byte, pub *rsa.PublicKey) []byte {
	hash := sha1.New()
	ciphertext, err := rsa.EncryptOAEP(hash, rand.Reader, pub, msg, nil)
	if err != nil {
		fmt.Println(err)
	}
	return ciphertext
}

// DecryptWithPrivateKey decrypts data with private key
func DecryptWithPrivateKey(ciphertext []byte, priv *rsa.PrivateKey) []byte {
	hash := sha1.New()
	plaintext, err := rsa.DecryptOAEP(hash, rand.Reader, priv, ciphertext, nil)
	if err != nil {
		fmt.Println(err)
	}
	return plaintext
} 

func SignWithPrivateKey(msg []byte, priv *rsa.PrivateKey) []byte {

	h := sha1.New()
	h.Write(msg)
	d := h.Sum(nil)

	ciphertext, err := rsa.SignPSS(rand.Reader, priv, crypto.SHA1, d, nil)
	if err != nil {
		fmt.Println(err)
	}
	return ciphertext
}

func VerifyWithPublicKey(msg []byte, sig []byte, pub *rsa.PublicKey) bool {

	h := sha1.New()
	h.Write(msg)
	d := h.Sum(nil)
	return rsa.VerifyPSS(pub, crypto.SHA1, d, sig, nil) == nil
}


// Encoding from: https://stackoverflow.com/questions/13573269/convert-string-to-byte

const maxInt32 = 1<<(32-1) - 1

func writeLen(b []byte, l int) []byte {
    if 0 > l || l > maxInt32 {
        panic("writeLen: invalid length")
    }
    var lb [4]byte
    binary.BigEndian.PutUint32(lb[:], uint32(l))
    return append(b, lb[:]...)
}

func readLen(b []byte) ([]byte, int) {
    if len(b) < 4 {
        panic("readLen: invalid length")
    }
    l := binary.BigEndian.Uint32(b)
    if l > maxInt32 {
        panic("readLen: invalid length")
    }
    return b[4:], int(l)
}

// func Decode(bb []byte) string {
	
// }

// func Encode(s []string) []byte {
//     var b []byte
//     b = writeLen(b, len(s))
//     for _, ss := range s {
//         b = writeLen(b, len(ss))
//         b = append(b, ss...)
//     }
//     return b
// }