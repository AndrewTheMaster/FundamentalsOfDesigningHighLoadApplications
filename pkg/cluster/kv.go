package cluster

type KV interface {
    PutString(key, value string) error
    GetString(key string) (string, bool, error)
    Delete(key string) error
}