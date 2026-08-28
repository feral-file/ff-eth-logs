package rpcapi

import "go.uber.org/zap"

func zapAddr(addr string) zap.Field { return zap.String("addr", addr) }
