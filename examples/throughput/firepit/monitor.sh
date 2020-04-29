#!/bin/bash
watch -n1 "date && redis-cli LLEN firepit:throughput:ready"
