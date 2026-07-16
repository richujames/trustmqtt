CC ?= gcc
# _GNU_SOURCE (equivalently _POSIX_C_SOURCE>=200112L) is required so strict
# -std=c11 still exposes the POSIX pthread_rwlock_* API that verdict_cache.c
# uses; without it the build fails on modern glibc ("unknown type name
# 'pthread_rwlock_t'").
CFLAGS = -std=c11 -D_GNU_SOURCE -Wall -Wextra -I../include
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
