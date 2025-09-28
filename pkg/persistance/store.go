package persistance

type Store struct {
	// TODO
}

func (ps *Store) Save(sst *SSTable) {

}

func (ps *Store) LoadL0() (*Iterator, error) {
	// TODO
	panic("TODO")
}

func (ps *Store) Load(level int) (*Iterator, error) {
	// TODO
	panic("TODO")
}

func (ps *Store) Compact(level int) error {
	// TODO
	panic("TODO")
}
