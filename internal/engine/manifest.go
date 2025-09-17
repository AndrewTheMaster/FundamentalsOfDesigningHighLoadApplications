package engine

// LevelMeta describes one LSM level.
type LevelMeta struct {
	Level int
	TableIDs []uint64
}

// ManifestState is the persisted description of tables and levels.
type ManifestState struct {
	NextTableID uint64
	Levels      []LevelMeta
}

// Manifest stores and atomically updates table metadata.
type Manifest interface {
	Load() (ManifestState, error)
	Apply(edit VersionEdit) (ManifestState, error)
}

// VersionEdit represents atomic changes to manifest.
type VersionEdit struct {
	NewTables   []uint64
	DeletedTables []uint64
	LevelPromotions map[uint64]int // tableID -> new level
} 