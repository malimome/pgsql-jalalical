
TARGET: pdate.so 

pdate.so : pdate.o pdatetime.o ptimestamp.o
	gcc -shared -o pdate.so pdate.o pdatetime.o ptimestamp.o

CFLAGS = -ggdb -g -I. -I/usr/local/pgsql/include/server \
  -I/usr/local/pgsql/include/internal -D_GNU_SOURCE

%.o : %.c
	gcc $(CFLAGS)   -c -o pdate.o pdate.c
	gcc $(CFLAGS)   -c -o pdatetime.o pdatetime.c
	gcc $(CFLAGS)   -c -o ptimestamp.o ptimestamp.c

pdate.sql: pdate.source
	rm -f $@; \
	C=`pwd`; \
	sed -e "s:_OBJWD_:$$C:g" < $< > $@

clean:
	rm -rvf *.so *.o *.sql
