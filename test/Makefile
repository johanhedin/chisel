CXX      := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2

ifdef SCHEMA

STEM    := $(basename $(SCHEMA))
HPP     := $(STEM).hpp
BIN     := $(STEM).bin
NS      := $(STEM)
DEFINES := -DCHISEL_HEADER='"$(HPP)"' -DCHISEL_NS=$(NS)

.PHONY: codec test clean

codec: $(HPP)

test: $(HPP) decode_test $(BIN)
	./decode_test $(BIN)

clean:
	rm -f decode_test $(HPP) $(BIN)

$(HPP): $(SCHEMA) ../chisel.py
	python3 ../chisel.py $<

$(BIN): $(SCHEMA) stream_gen.py
	python3 stream_gen.py $< --seed 42 -n 20

decode_test: decode_test.cpp $(HPP)
	$(CXX) $(CXXFLAGS) $(DEFINES) $< -o $@

else

.PHONY: codec test clean
codec test clean:
	$(error SCHEMA is not set. Usage: make SCHEMA=<schema.json> [codec|test|clean])

endif

.PHONY: allclean
allclean:
	rm -f decode_test *.hpp *.bin
