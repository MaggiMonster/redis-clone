#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(6379);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);   // connect to localhost

    connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    write(fd, "*1\r\n$4\r\n", 8);   // first half
    sleep(1);                       // force a gap between the two reads
    write(fd, "PING\r\n", 6);       // the rest

    char buf[100];
    ssize_t n = read(fd, buf, sizeof(buf));
    std::cout << "got " << n << " bytes: ";
    std::cout.write(buf, n);        // expect +PONG\r\n
    std::cout << '\n';

    close(fd);
    return 0;
}