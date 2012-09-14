package dynamodb

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"goamz/aws"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const ISO8601BasicShortFormat = "20060102"

var b64 = base64.StdEncoding

func (s *Server) Sign(host string, params url.Values, headers http.Header) []string {
	auth := &s.Auth
	server := *s
	now    := time.Now().UTC()
	secret := server.derivedSecret(now)
	hash := hmac.New(sha256.New, secret)

	createSignature(hash, params, headers)

	auth := bytes.NewBufferString("AWS4-HMAC-SHA256 ")
	auth.Write([]byte("Credential=" + auth.AccessKey + "/" + server.credentials(now) + ", "))
	auth.Write([]byte("SignedHeaders="))
	auth.Write(headerList(headers))
	auth.Write([]byte(", "))
	auth.Write([]byte("Signature=" + fmt.Sprintf("%x", hash.Sum(nil))))

	return auth.String()
}

func (s *Server) derivedSecret(now time.Time) []byte {
	auth := *s.Auth
	h := lhmac([]byte("AWS4" + auth.SecretKey), []byte(now.Format(ISO8601BasicShortFormat)))
	h = lhmac(h, []byte(s.Region.Name))
	h = lhmac(h, []byte("dynamodb"))
	h = lhmac(h, []byte("aws4_request"))

	return h	
}

// a local hmac for use in creating a derived secret.
func lhmac(key, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func (s * Server) credentials(now time.Time) string{
	return t.Format(ISO8601BasicShortFormat) + "/" + s.Region + "/dynamodb/aws4_request"
}

func headerList(headers http.Header) []byte {
	i := 0
	a := make([]string, len(headers))

	for key, _ := range headers {
		a[i] = strings.ToLower(k)
		i++
	}

	sort.Strings(a)

	return a.Join(";")
}

func createSignature(hash hash.Hash,  params url.Values, headers http.Header){

	sig := bytes.NewBufferString("AWS4-HMAC-SHA256\n")	
	sig.write([]byte(

}
