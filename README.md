# junyul-go

Junyul compliance SDK for Go.

Status: GA. Latest verified release: `v1.0.2`.

Module path:

```text
github.com/junyul-go/junyul-go
```

Install:

```bash
go get github.com/junyul-go/junyul-go@latest
```

Local verification:

```bash
cd sdks/go
go test ./...
```

```go
import "github.com/junyul-go/junyul-go"

client, _ := junyul.New("JUN_live_xxx")
defer client.Close()

reply, err := junyul.Track(ctx, client, "gpt_chatbot_v1", func() (string, error) {
    return callOpenAI(ctx, message)
})
```
