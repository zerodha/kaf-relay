package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
)

type checkPoint struct {
	GroupID string
	Offsets map[string]map[int32]kgo.EpochOffset
}

// loadCheckpoint loads the groupid, offsets to relay
func loadCheckpoint(path string, manager *consumerManager, l *slog.Logger) error {
	// ignore if file is not available
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		l.Info("checkpoint file does not exist", "path", path)
		return nil
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var c checkPoint
	dec := gob.NewDecoder(f)
	err = dec.Decode(&c)
	if err != nil && err != io.EOF {
		log.Printf("error gob loading checkpoint: %v\n", err)
		return err
	}

	// empty file? return empty checkpoint
	if err == io.EOF {
		return nil
	}

	l.Info("loaded checkpoint", "group_id", c.GroupID, "offsets", c.Offsets)

	// set the group id, offsets under consumer manager
	manager.Lock()
	defer manager.Unlock()

	if c.Offsets != nil {
		manager.setOffsets(c.Offsets)
	}

	// get the group from the cfgs and set is as active
	if c.GroupID != "" {
		idx := -1

		for i, cfg := range manager.c.cfgs {
			if cfg.GroupID == c.GroupID {
				idx = i
				break
			}
		}

		foundGroup := idx != -1
		// check if given broker is up and set
		_, ok := manager.brokersUp[c.GroupID]
		if foundGroup {
			switch ok {
			case true:
				manager.setActive(idx)
			case false:
				b := manager.c.cfgs[idx].BootstrapBrokers
				return fmt.Errorf("%w: %v", ErrBrokerUnavailable, b)
			}
		} else {
			return fmt.Errorf("invalid `group_id`: %v", c.GroupID)
		}
	}

	return nil
}

// saveCheckpoint gob dumps the group id, offsets back to checkpoint file.
func saveCheckpoint(path string, manager *consumerManager, l *slog.Logger) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	manager.Lock()
	cfg := manager.getCurrentConfig()
	o := manager.getOffsets()
	manager.Unlock()

	c := checkPoint{
		GroupID: cfg.GroupID,
		Offsets: o,
	}

	enc := gob.NewEncoder(f)
	err = enc.Encode(c)
	if err != nil {
		log.Printf("error gob dumping checkpoint: %v\n", err)
		return err
	}

	l.Info("saved checkpoint", "group_id", c.GroupID, "offsets", c.Offsets)

	return nil
}
