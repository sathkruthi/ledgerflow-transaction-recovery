# Root cause

The legacy test path reads without account locks, synchronizes both stale snapshots, then overwrites the source cache with an absolute value. Both journals post; the source cache remains 25 while its journal-derived balance is -50. Correct processing locks account rows in consistent order and reevaluates available funds after the first commit.
