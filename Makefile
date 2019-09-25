# Compiler and Compile options.
CC = g++ 
CXXFLAGS = -g -Wall -std=c++11 -O2

# Macros specifying path for compile.
SRCS := $(wildcard src/*.cpp)
OBJS := $(SRCS:.cpp=.o)
DEPS := $(wildcard *.h)

# Compile command.
TARGET = run
$(TARGET): $(OBJS)
	$(CC) $(CXXFLAGS) -o $(TARGET) $(OBJS)
$(TARGET).o: $(DEPS)

# Delete binary & object files.
clean:
	rm $(TARGET) $(OBJS)