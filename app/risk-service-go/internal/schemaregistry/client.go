package schemaregistry

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/linkedin/goavro/v2"
)

const magicByte byte = 0

type Client struct {
	// Client кэширует Avro codecs, чтобы не ходить в Schema Registry на каждое сообщение.
	baseURL    string
	httpClient *http.Client

	mu             sync.RWMutex
	codecByID      map[int]*goavro.Codec
	codecBySubject map[string]subjectCodec
}

type subjectCodec struct {
	// Для записи нужен и codec, и schema id, который попадет в Kafka payload.
	id    int
	codec *goavro.Codec
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{
		baseURL:        baseURL,
		httpClient:     httpClient,
		codecByID:      map[int]*goavro.Codec{},
		codecBySubject: map[string]subjectCodec{},
	}
}

func (c *Client) Decode(ctx context.Context, payload []byte) (map[string]interface{}, error) {
	// Confluent wire format: 1 byte magic + 4 bytes schema id + Avro binary.
	if len(payload) < 5 || payload[0] != magicByte {
		return nil, fmt.Errorf("payload is not in Confluent Avro wire format")
	}

	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	codec, err := c.codecForID(ctx, schemaID)
	if err != nil {
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(payload[5:])
	if err != nil {
		return nil, fmt.Errorf("decode avro payload with schema id %d: %w", schemaID, err)
	}

	// goavro возвращает record как map[string]interface{}.
	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("decoded payload is %T, expected map", native)
	}
	return record, nil
}

func (c *Client) Encode(ctx context.Context, subject string, record map[string]interface{}) ([]byte, error) {
	// Для публикации берем последнюю registered schema по subject.
	entry, err := c.codecForSubject(ctx, subject)
	if err != nil {
		return nil, err
	}

	binaryPayload, err := entry.codec.BinaryFromNative(nil, record)
	if err != nil {
		return nil, fmt.Errorf("encode avro payload for %s: %w", subject, err)
	}

	buffer := bytes.NewBuffer(make([]byte, 0, len(binaryPayload)+5))
	// KafkaJS SchemaRegistry codec пишет такой же Confluent prefix.
	buffer.WriteByte(magicByte)
	_ = binary.Write(buffer, binary.BigEndian, uint32(entry.id))
	buffer.Write(binaryPayload)
	return buffer.Bytes(), nil
}

func (c *Client) codecForID(ctx context.Context, schemaID int) (*goavro.Codec, error) {
	// Сначала быстрый путь: schema id уже встречался в этом процессе.
	c.mu.RLock()
	codec := c.codecByID[schemaID]
	c.mu.RUnlock()
	if codec != nil {
		return codec, nil
	}

	schema, err := c.getSchemaByID(ctx, schemaID)
	if err != nil {
		return nil, err
	}
	codec, err = goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("compile schema id %d: %w", schemaID, err)
	}

	c.mu.Lock()
	c.codecByID[schemaID] = codec
	c.mu.Unlock()
	return codec, nil
}

func (c *Client) codecForSubject(ctx context.Context, subject string) (subjectCodec, error) {
	// Subject используется при записи, например risk.risk-events-OrderRiskApproved-value.
	c.mu.RLock()
	entry, ok := c.codecBySubject[subject]
	c.mu.RUnlock()
	if ok {
		return entry, nil
	}

	id, schema, err := c.getLatestSchema(ctx, subject)
	if err != nil {
		return subjectCodec{}, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return subjectCodec{}, fmt.Errorf("compile latest schema for %s: %w", subject, err)
	}

	entry = subjectCodec{id: id, codec: codec}
	c.mu.Lock()
	c.codecBySubject[subject] = entry
	c.codecByID[id] = codec
	c.mu.Unlock()
	return entry, nil
}

func (c *Client) getSchemaByID(ctx context.Context, schemaID int) (string, error) {
	var response struct {
		Schema string `json:"schema"`
	}
	if err := c.getJSON(ctx, "/schemas/ids/"+strconv.Itoa(schemaID), &response); err != nil {
		return "", err
	}
	return response.Schema, nil
}

func (c *Client) getLatestSchema(ctx context.Context, subject string) (int, string, error) {
	var response struct {
		ID     int    `json:"id"`
		Schema string `json:"schema"`
	}
	if err := c.getJSON(ctx, "/subjects/"+subject+"/versions/latest", &response); err != nil {
		return 0, "", err
	}
	return response.ID, response.Schema, nil
}

func (c *Client) getJSON(ctx context.Context, path string, target interface{}) error {
	// Все запросы идут с context, чтобы shutdown мог отменить сетевой вызов.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(response.Body)
		return fmt.Errorf("schema registry %s returned %d: %s", path, response.StatusCode, string(body))
	}
	return json.NewDecoder(response.Body).Decode(target)
}
