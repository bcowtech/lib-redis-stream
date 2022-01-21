
## Preparation

### 1. Install godotenv tool

```bash
$ go get -v -t github.com/joho/godotenv/cmd/godotenv 
```

### 2. Configurate Your `.env` File from `.env.sample`

```bash
$ cp .env.sample .env
```
> ⚠️ Don't modify the `.env.sample` as your `go run` source

```bash
$ vim .env
```

### 3. Configurate Your redis and feed sample data

```bash
127.0.0.1:6379> XGROUP CREATE demo-stream demo-group 0 MKSTREAM
OK
127.0.0.1:6379> EVAL 'for i=1,100000 do redis.call("XADD", "demo-stream", "*", "value", i) end' 0
(nil)
127.0.0.1:6379> XLEN demo-stream
(integer) 100000
```

--------
## Run Demo

```bash
$ godotenv -f .env go run main.go
```
> 👣 You can break the program using Ctrl+C.
