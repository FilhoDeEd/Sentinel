import asyncio
from shared.protocol import parse_status_update

HOST = '0.0.0.0'
PORT = 8888

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    data = await reader.read(100)
    message = data.decode().strip()

    print(f'Received {message!r} from {addr!r}')

    status_update = parse_status_update(message)
    if status_update:
        print(f'Valid update: {status_update}')

    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)

    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        try:
            await server.serve_forever()
        except asyncio.CancelledError:
            print('Shutting down gracefully...')

asyncio.run(main())
