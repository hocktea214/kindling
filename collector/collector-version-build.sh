GitCommit=$(git rev-parse --short HEAD || echo unsupported)
echo "Git commit:" $GitCommit
export GOPROXY=https://goproxy.cn
go build -o docker/kindling-collector -ldflags="-X 'github.com/Kindling-project/kindling/collector/version.CodeVersion=$GitCommit'" ./cmd/kindling-collector/


