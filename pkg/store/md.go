package store

type MD uint64

func newMD(op operation, valType valType) MD {
	return MD(uint64(valType)<<8 | uint64(op))
}

func (md MD) operation() operation {
	return operation(uint64(md) & 0xff)
}

func (md MD) valType() valType {
	return valType(md >> 8)
}
