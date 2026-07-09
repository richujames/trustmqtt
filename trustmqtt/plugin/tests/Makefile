CC ?= gcc
CFLAGS = -std=c11 -Wall -Wextra -I../include
LDFLAGS = -lpthread

.PHONY: test clean

test: test_ring test_verdict_cache test_token_bucket test_enforce
	./test_ring
	./test_verdict_cache
	./test_token_bucket
	./test_enforce

test_ring: test_ring.c ../src/ring.c
	$(CC) $(CFLAGS) -o $@ test_ring.c ../src/ring.c $(LDFLAGS)

test_verdict_cache: test_verdict_cache.c ../src/verdict_cache.c
	$(CC) $(CFLAGS) -o $@ test_verdict_cache.c ../src/verdict_cache.c $(LDFLAGS)

test_token_bucket: test_token_bucket.c ../src/verdict_cache.c
	$(CC) $(CFLAGS) -o $@ test_token_bucket.c ../src/verdict_cache.c $(LDFLAGS)

test_enforce: test_enforce.c ../src/enforce.c ../src/verdict_cache.c
	$(CC) $(CFLAGS) -o $@ test_enforce.c ../src/enforce.c ../src/verdict_cache.c $(LDFLAGS)

clean:
	rm -f test_ring test_verdict_cache test_token_bucket test_enforce *.exe
