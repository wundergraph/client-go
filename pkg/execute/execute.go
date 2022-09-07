package execute

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func Query[Input any, Response any](client *http.Client, ctx context.Context, baseURL, path string, input *Input) (response *Response, err error) {
	baseUrlWithPath := baseURL + path
	if input != nil {
		variables, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		baseUrlWithPath = baseUrlWithPath + "?wg_variables=" + url.QueryEscape(string(variables))
	}
	req, err := http.NewRequestWithContext(ctx, "GET", baseUrlWithPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		if _, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("connection refused: %s://%s", req.URL.Scheme, req.URL.Host)
		}
		return nil, err
	}
	if res.StatusCode == http.StatusOK {
		defer res.Body.Close()
		err = json.NewDecoder(res.Body).Decode(&response)
		return response, nil
	}
	if res.StatusCode == http.StatusBadRequest {
		return nil, errors.New("bad request")
	}
	if res.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("unauthorized")
	}
	if res.StatusCode == http.StatusInternalServerError {
		return nil, errors.New("internal server error")
	}
	return nil, errors.New("unknown error")
}

func Mutate[Input any, Response any](client *http.Client, ctx context.Context, baseURL, path string, input *Input) (response *Response, err error) {
	baseUrlWithPath := baseURL + path
	var (
		body *bytes.Buffer
	)
	if input != nil {
		body = &bytes.Buffer{}
		err = json.NewEncoder(body).Encode(input)
		if err != nil {
			return nil, errors.New("error encoding input")
		}
	}
	req, err := http.NewRequestWithContext(ctx, "POST", baseUrlWithPath, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		if _, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("connection refused: %s://%s", req.URL.Scheme, req.URL.Host)
		}
		return nil, err
	}
	if res.StatusCode == http.StatusOK {
		defer res.Body.Close()
		err = json.NewDecoder(res.Body).Decode(&response)
		return response, nil
	}
	if res.StatusCode == http.StatusBadRequest {
		return nil, errors.New("bad request")
	}
	if res.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("unauthorized")
	}
	if res.StatusCode == http.StatusInternalServerError {
		return nil, errors.New("internal server error")
	}
	return nil, errors.New("unknown error")
}

func LiveQuery[Input any, Response any](client *http.Client, ctx context.Context, baseURL, path string, input *Input) (*Stream[Response], error) {
	return buildStream[Input, Response](client, ctx, baseURL, path, true, input)
}

func Subscribe[Input any, Response any](client *http.Client, ctx context.Context, baseURL, path string, input *Input) (*Stream[Response], error) {
	return buildStream[Input, Response](client, ctx, baseURL, path, false, input)
}

func buildStream[Input any, Response any](client *http.Client, ctx context.Context, baseURL, path string, liveQuery bool, input *Input) (*Stream[Response], error) {
	baseUrlWithPath := baseURL + path
	if input != nil {
		variables, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		baseUrlWithPath = baseUrlWithPath + "?wg_variables=" + url.QueryEscape(string(variables))
		if liveQuery {
			baseUrlWithPath += "&wg_live=true"
		}
	} else if liveQuery {
		baseUrlWithPath = baseUrlWithPath + "?wg_live=true"
	}
	req, err := http.NewRequestWithContext(ctx, "GET", baseUrlWithPath, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := client.Do(req)
	if err != nil {
		if _, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("connection refused: %s://%s", req.URL.Scheme, req.URL.Host)
		}
		return nil, err
	}
	if res.StatusCode == http.StatusOK {
		return &Stream[Response]{
			body:   res.Body,
			reader: bufio.NewReader(res.Body),
			buf:    &bytes.Buffer{},
		}, nil
	}
	if res.StatusCode == http.StatusBadRequest {
		return nil, errors.New("bad request")
	}
	if res.StatusCode == http.StatusUnauthorized {
		return nil, errors.New("unauthorized")
	}
	if res.StatusCode == http.StatusInternalServerError {
		return nil, errors.New("internal server error")
	}
	return nil, errors.New("unknown error")
}

type Stream[Response any] struct {
	body   io.ReadCloser
	reader *bufio.Reader
	buf    *bytes.Buffer
}

func (s *Stream[Response]) Close() error {
	if s == nil || s.body == nil {
		return nil
	}
	return s.body.Close()
}

func (s *Stream[Response]) Next(ctx context.Context) (res *Response, closed bool, err error) {
	defer func() {
		// if we cancel the context, the server can close the stream while sending the next response
		// this might lead to unexpected errors which we'd like to catch, because it would be unexpected
		// this defer func simply cleans up the return values in case of a context cancelation
		if ctx.Err() != nil {
			err = nil
			closed = true
		}
	}()
	if s == nil || s.buf == nil || s.reader == nil {
		_ = s.Close()
		return nil, true, errors.New("stream is closed")
	}
	s.buf.Reset()
	var (
		lastByteIsNewLine = false
	)
	for {
		if err := ctx.Err(); err != nil {
			// context canceled, stop reading
			_ = s.Close()
			return nil, true, nil
		}
		b, err := s.reader.ReadByte()
		if err != nil {
			_ = s.Close()
			return nil, true, errors.New("unexpected end of stream")
		}
		if b == '\n' {
			// potential end of message
			if lastByteIsNewLine {
				// end of message detected (\n\n)
				var response Response
				err = json.NewDecoder(s.buf).Decode(&response)
				if err != nil {
					_ = s.Close()
					return nil, true, errors.New("error reading JSON")
				}
				return &response, false, nil
			}
			// note that we have a newline
			lastByteIsNewLine = true
			continue
		}
		if lastByteIsNewLine {
			// only single newline, write to buffer
			err = s.buf.WriteByte('\n')
			if err != nil {
				_ = s.Close()
				return nil, true, errors.New("buffer overflow")
			}
		}
		lastByteIsNewLine = false
		err = s.buf.WriteByte(b)
		if err != nil {
			_ = s.Close()
			return nil, true, errors.New("buffer overflow")
		}
	}
}
