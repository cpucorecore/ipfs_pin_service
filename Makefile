.PHONY: all build clean proto integration

# 设置 Go 编译器和标志
GO := go
BINARY_NAME := ipfs_pin_service
MAIN_PATH := ./cmd/ipfs_pin_service/main.go
BIN_DIR := bin

# protoc 编译器和选项
PROTOC := protoc
PROTO_FILES := $(wildcard proto/*.proto)
PROTO_GO_OUT := .
PROTO_GO_OPT := paths=source_relative

all: proto build

# 编译二进制文件
build:
	mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/$(BINARY_NAME) $(MAIN_PATH)

# 生成 protobuf 代码
proto:
	$(PROTOC) --go_out=$(PROTO_GO_OUT) --go_opt=$(PROTO_GO_OPT) $(PROTO_FILES)

# 清理生成的文件
clean:
	rm -f $(BIN_DIR)/$(BINARY_NAME)
	rm -f proto/*.pb.go

# 安装依赖
deps:
	$(GO) mod tidy
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# 运行测试
test:
	$(GO) test ./...

# Run integration tests that require local RabbitMQ
integration:
	IT_RABBITMQ=1 $(GO) test ./internal/queue -tags=integration -count=1

# 帮助信息
help:
	@echo "可用的 make 命令："
	@echo "  make          - 生成 protobuf 代码并编译项目"
	@echo "  make build    - 编译项目"
	@echo "  make proto    - 生成 protobuf 代码"
	@echo "  make clean    - 清理生成的文件"
	@echo "  make deps     - 安装依赖"
	@echo "  make test     - 运行测试"
	@echo "  make integration - 运行需要本地 RabbitMQ 的集成测试"
	@echo "  make help     - 显示帮助信息"
