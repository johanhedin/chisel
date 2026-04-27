CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2

ifdef SCHEMA

STEM             := $(basename $(SCHEMA))
HPP              := $(STEM).hpp
TEST_HPP         := $(STEM)_test.hpp
LAZY_FILTER_HPP  := $(STEM)_lazy_filter.hpp
BIN              := $(STEM).bin
BIN_ENC          := $(STEM)_enc.bin
ROOT             := $(shell python3 -c "import json; print(json.load(open('$(SCHEMA)'))['name'])")
DEFINES          := -DCHISEL_HEADER='"$(HPP)"' -DCHISEL_ROOT=$(ROOT)
DEFINES_ENC      := -DCHISEL_TEST_HEADER='"$(TEST_HPP)"' -DCHISEL_ROOT=$(ROOT)
DEFINES_LAZY     := -DCHISEL_HEADER='"$(HPP)"' -DCHISEL_ROOT=$(ROOT) \
                    -DCHISEL_LAZY_FILTER='"$(LAZY_FILTER_HPP)"'

.PHONY: codec test encode-test lazy-test clean

codec: $(HPP)

test: $(HPP) decode_test $(BIN) encode_test $(BIN_ENC) lazy_test
	./decode_test $(BIN)
	python3 stream_read.py $(SCHEMA) $(BIN_ENC)
	./lazy_test $(BIN)

encode-test: encode_test $(BIN_ENC)
	python3 stream_read.py $(SCHEMA) $(BIN_ENC)

lazy-test: lazy_test $(BIN)
	./lazy_test $(BIN)

clean:
	rm -f decode_test encode_test lazy_test $(HPP) $(TEST_HPP) $(BIN) $(BIN_ENC)

$(HPP): $(SCHEMA) ../chisel.py
	python3 ../chisel.py $<

$(TEST_HPP): $(SCHEMA) ../chisel.py
	python3 ../chisel.py --test-helpers $<

$(BIN): $(SCHEMA) stream_gen.py
	python3 stream_gen.py $< --seed 42 -n 20

$(BIN_ENC): encode_test
	./encode_test $(BIN_ENC) 20 42

decode_test: decode_test.cpp $(HPP)
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@

encode_test: encode_test.cpp $(HPP) $(TEST_HPP)
	$(CXX) $(CXXFLAGS) $(DEFINES_ENC) $< -o $@

lazy_test: lazy_test.cpp $(HPP) $(LAZY_FILTER_HPP)
	$(CXX) $(CXXFLAGS) $(DEFINES_LAZY) $< -o $@

else

.PHONY: codec test clean
codec test clean:
	$(error SCHEMA is not set. Usage: make SCHEMA=<schema.json> [codec|test|clean])

endif

.PHONY: allclean
allclean:
	rm -f decode_test encode_test lazy_test *.bin
