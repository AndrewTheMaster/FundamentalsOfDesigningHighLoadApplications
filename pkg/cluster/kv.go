package cluster

type KV interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

type KVService struct {
	kv KV // это может быть либо локальный store.Store, либо Router
}

func NewKVService(kv KV) *KVService {
	return &KVService{kv: kv}
}
