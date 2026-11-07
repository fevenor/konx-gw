package main

import (
	"io"
	"net/http"
	"time"
)

type httpTransport struct{}

func (httpTransport) get(url string) (string, error) {
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	return string(b), err
}
