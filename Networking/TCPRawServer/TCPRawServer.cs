using System;
using System.Collections.Generic;
using System.Text;
using WFast.Memory;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using PNLogService.Logs;

namespace WFast.Networking
{
    public delegate 
        void ServerStartEvent(IPAddress listenIp, ushort listenPort, int bckLog, bool isSuccess, int retryTime);
    public delegate 
        void ServerAccessDeniedEvent(EndPoint ep, int emptySlots, bool isFull);

    public delegate 
        bool ServerConnectEvent(int clientFd, EndPoint remotePoint, out object clientData);
    public delegate 
        void ServerDisconnectEvent(int clientFd, object clientData, string disconnectReason);
    public delegate 
        void ServerOnReceivePacketEvent(byte[] packet, int clientFd, object clientData);
    public unsafe delegate 
        int PacketCalculateEvent(byte* buffer);
    public delegate 
        void ServerExceptionEvent(Exception e);
    public delegate
        void ClientAttemptDDosEvent(int clientId, int bytesInBuffer);
    internal class client
    {
        public int clientId;
        public object clientData;
        public Socket clientSock;
        public MemoryStream sendStream;

        public int bytesReceived;
        public byte[] rcvBuff;
        public bool headerReceived;

        public bool IsWriteing;
        public bool IsReading;

        public client(Socket sock, int clientId, object clientData, int recvBufferSize, int sendStreamSize)
        {
            headerReceived = false;
            bytesReceived = 0;
            IsWriteing = false;
            IsReading = false;
            this.clientId = clientId;
            this.clientData = clientData;
            clientSock = sock;
            sendStream = new MemoryStream(sendStreamSize);
            rcvBuff = new byte[recvBufferSize];
        }
    }

    public unsafe class TCPRawServer
    {
        enum serverCommandTypes : byte { disconnect = 1, send_bytes = 2 }
        struct serverCommand
        {
            public int clientFD;

            public serverCommandTypes type;

            public byte[] sndBuffer;
            public int sendOffset;
            public int countBytes;

            public static serverCommand makeDisconnectCommand(int clientFd)
            {
                return new serverCommand()
                {
                    clientFD = clientFd,
                    type = serverCommandTypes.disconnect
                };
            }
            public static serverCommand makeSendPacketCommand(int clientFd, byte[] buff, int offset, int cb)
            {
                return new serverCommand()
                {
                    clientFD = clientFd,
                    type = serverCommandTypes.send_bytes,
                    sndBuffer = buff,
                    sendOffset = offset,
                    countBytes = cb
                };
            }
        }

        private PacketCalculateEvent calculatePacketSize;
        private ServerConnectEvent onNewClientEv;
        private ServerDisconnectEvent onDisconnectClientEv;
        private ServerOnReceivePacketEvent onNewPacketEv;

        public ServerAccessDeniedEvent OnAccessDenied;
        public ServerStartEvent OnServerStarted;
        public ServerExceptionEvent OnServerException;
        public ClientAttemptDDosEvent OnClientDDos;

        private int connRetryTimeout;
        private IPAddress listenIp;
        private ushort listenPort;
        private Socket listenSock;
        private int backLog;
        private double sendRecvRatio;
        private int sendStreamSize;
        private int packetHeaderSize;
        private int maxPacketSize;
        private int maxClients;
        private int maxPacketsPerStep;
        private int serverTickMicro;
        private bool noCheckDDos;

        private client[] clients;
        private Stack<int> freeClientSlots;
        private List<Socket> toReadSockets;
        private List<Socket> toWriteSockets;

        private Dictionary<Socket, int> clientAssocList;

        public int Online { get; private set; }

        private Thread workerThread;
        private bool serverStarted;

        private ConcurrentQueue<serverCommand> commandsQueue;
        private Queue<serverCommand> secondCommandsQueue;

        public TCPRawServer(
            IPAddress _listenIp, ushort _listenPort,
            int _backLg,
            int headerSize,
            PacketCalculateEvent _calculatePacketSize,
            ServerConnectEvent _connectHandler = null,
            ServerDisconnectEvent _disconnectHandler = null,
            ServerOnReceivePacketEvent _recvPacketHandler = null,
            int _maxConnections = 4096,
            int _maxPacketSize = 64 * 1024,
            int _maxPacketsPerStep = 32,
            double _sendRecvRatio = 3 / 1,
            bool _noCheckDDos = false,
            int _serverTickMicro = 5000,
            int _connRetry = 5000)
        {
            this.listenIp = _listenIp;
            this.listenPort = _listenPort;
            this.backLog = _backLg;

            this.packetHeaderSize = headerSize;
            this.maxClients = _maxConnections;
            this.maxPacketSize = _maxPacketSize;
            this.maxPacketsPerStep = _maxPacketsPerStep;
            this.noCheckDDos = _noCheckDDos;
            this.serverTickMicro = _serverTickMicro;
            this.sendRecvRatio = _sendRecvRatio;
            this.connRetryTimeout = _connRetry;

            this.calculatePacketSize = _calculatePacketSize;
            this.onNewClientEv = _connectHandler;
            this.onNewPacketEv = _recvPacketHandler;
            this.onDisconnectClientEv = _disconnectHandler;

            if (this.calculatePacketSize == null)
                throw new ArgumentNullException("PacketCalculateEvent is null");

            toReadSockets = new List<Socket>(maxClients);
            toWriteSockets = new List<Socket>(maxClients);
            clientAssocList = new Dictionary<Socket, int>(maxClients);
            commandsQueue = new ConcurrentQueue<serverCommand>();
            secondCommandsQueue = new Queue<serverCommand>();

            sendStreamSize = (int)Math.Ceiling((maxPacketSize * maxPacketsPerStep) * sendRecvRatio);

            if (sendStreamSize <= maxPacketSize)
                sendStreamSize = maxPacketSize + 1;

            clients = new client[maxClients];
            freeClientSlots = new Stack<int>();
            Online = 0;

            for (int i = 0; i < maxClients; ++i)
            {
                clients[i] = null;
                freeClientSlots.Push(i);
            }

            serverStarted = false;
            workerThread = null;
        }

        public void StartServer(bool inThisThread = false, ThreadPriority threadPriority = ThreadPriority.Highest)
        {
            if (workerThread != null || serverStarted)
                throw new Exception("Error StartServer() server already started");

            if (inThisThread)
            {
                worker();
                return;
            }

            workerThread = new Thread(worker);

            workerThread.IsBackground = true;
            workerThread.Priority = threadPriority;

            workerThread.Start();
        }

        private int getFreeClientId()
        {
            if (!freeClientSlots.TryPop(out int newClientId))
                return -1;
            else
                return newClientId;
        }
        private void returnClientId(int clientId)
        {
            freeClientSlots.Push(clientId);
        }

        private void handleCommand(serverCommand cmd)
        {
            if (cmd.type == serverCommandTypes.send_bytes)
                sendbytesClientHandler(cmd.clientFD, cmd.sndBuffer, cmd.sendOffset, cmd.countBytes);
            else if (cmd.type == serverCommandTypes.disconnect)
                disconnectClientHandler(cmd.clientFD);
        }

        public void SendBytesToClient(int clientId, byte[] buffer, int offset, int countBytes)
        {
            if (clientId < 0 || clientId >= maxClients)
                return;

            commandsQueue.Enqueue(serverCommand.makeSendPacketCommand(clientId, buffer, offset, countBytes));
        }
        public void DisconnectClient(int clientId)
        {
            if (clientId < 0 || clientId >= maxClients)
                return;

            commandsQueue.Enqueue(serverCommand.makeDisconnectCommand(clientId));
        }

        private void sendbytesClientHandler(int clientId, byte[] buffer, int offset, int countBytes)
        {
            client thisClient = clients[clientId];

            if (thisClient == null)
                return;

            try
            {
                fixed (byte* pinnedBuffer = buffer)
                {
                    thisClient.sendStream.Write((IntPtr)(pinnedBuffer + offset), countBytes);
                }
            }
            catch (MemoryStreamIsFullException e)
            {
                secondCommandsQueue.Enqueue(serverCommand.makeSendPacketCommand(clientId, buffer, offset, countBytes));
                //callServerEvent($"Error push packet to client ({clientId}) memoryStream is full [{e.Message}] pushing in SecondQueue [size={secondCommandsQueue.Count}]");
            }
            setClientToWrite(thisClient);
        }
        private void disconnectClientHandler(int clientId)
        {
            client thisClient = clients[clientId];

            if (thisClient == null) return;

            onDisconnectEvent(thisClient, "Connection destroyed by Server");
        }

        private void setClientToRead(client thisClient)
        {
            if (!thisClient.IsReading)
            {
                thisClient.IsReading = true;
                toReadSockets.Add(thisClient.clientSock);
            }
        }
        private void setClientToWrite(client thisClient)
        {
            if (!thisClient.IsWriteing)
            {
                thisClient.IsWriteing = true;
                toWriteSockets.Add(thisClient.clientSock);
            }
        }
        private void stopReadClient(client thisClient)
        {
            if (thisClient.IsReading)
            {
                thisClient.IsReading = false;
                toReadSockets.Remove(thisClient.clientSock);
            }
        }
        private void stopWriteClient(client thisClient)
        {
            if (thisClient.IsWriteing)
            {
                thisClient.IsWriteing = false;
                toWriteSockets.Remove(thisClient.clientSock);
            }
        }

        private void onConnectEvent(Socket connectedSocket)
        {
            int clientId = getFreeClientId();

            if (clientId == -1)
            {
                if (OnAccessDenied != null)
                    OnAccessDenied(connectedSocket.RemoteEndPoint, freeClientSlots.Count, true);
                //callServerEvent($"Client [{connectedSocket.RemoteEndPoint}] denied for access because server is full [Online is {Online}] [Empty slots is {freeClientSlots.Count}]");
                connectedSocket.Close();
                return;
            }
            bool canConnect = true;
            object clientData = null;

            if (onNewClientEv != null)
                canConnect = onNewClientEv(clientId, connectedSocket.RemoteEndPoint, out clientData);

            if (!canConnect)
            {
                if (OnAccessDenied != null)
                    OnAccessDenied(connectedSocket.RemoteEndPoint, freeClientSlots.Count, false);
                connectedSocket.Close();
                returnClientId(clientId);
                return;
            }

            connectedSocket.Blocking = false;
            connectedSocket.ReceiveTimeout = 2000;
            connectedSocket.SendTimeout = 2000;
            connectedSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
            connectedSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, 1);
            connectedSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 10);
            connectedSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 1);
            connectedSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 1);

            client newClient = new client(connectedSocket, clientId, clientData, maxPacketSize, sendStreamSize);

            clients[newClient.clientId] = newClient;
            clientAssocList.Add(newClient.clientSock, newClient.clientId);
            setClientToRead(newClient);

            Online++;
        }
        private void onDisconnectEvent(client thisClient, string reason)
        {
            clients[thisClient.clientId] = null;
            clientAssocList.Remove(thisClient.clientSock);

            stopReadClient(thisClient);
            stopWriteClient(thisClient);

            Online--;

            thisClient.clientSock.Close();

            if (onDisconnectClientEv != null)
                onDisconnectClientEv(thisClient.clientId, thisClient.clientData, reason);

            returnClientId(thisClient.clientId);

            thisClient = null;
        }
        private void handleSocketRead(client thisClient)
        {
            byte[] rcvBuff = thisClient.rcvBuff;

            int getPacketSize()
            {
                fixed (byte* pinnedBuffer = rcvBuff)
                    return calculatePacketSize(pinnedBuffer);
            }

            Socket clientSock = thisClient.clientSock;

            int needToRecv = 0;
            int packetsHandled = 0;
            int tmpRecv = 0;
            int bytesInBuffer = 0;
            SocketError codeError;

            string disconnectReason = "Connection reset.";

            bytesInBuffer = clientSock.Available;

            if (bytesInBuffer > (maxPacketSize * 128) && !noCheckDDos)
            {
                if (OnClientDDos != null)
                    OnClientDDos(thisClient.clientId, bytesInBuffer);
                //callServerEvent($"Client {thisClient.clientId} detected attempt to ddos too much buffer: ({bytesInBuffer}).");
                disconnectReason = "Client attempt to DDOS [too big data in buffer]";
                goto on_disconnect;
            }
            else if (bytesInBuffer == 0)
            {
                goto on_disconnect;
            }
            for (; ; )
            {
                needToRecv = !thisClient.headerReceived ?
                                (packetHeaderSize - thisClient.bytesReceived) :
                                (getPacketSize() - thisClient.bytesReceived);

                if (packetsHandled == maxPacketsPerStep)
                    return;

                if (needToRecv != 0)
                {
                    tmpRecv = clientSock.Receive(rcvBuff, thisClient.bytesReceived, needToRecv, SocketFlags.None, out codeError);

                    if (codeError != SocketError.Success || tmpRecv == 0)
                    {
                        if (codeError == SocketError.WouldBlock)
                            return;
                        else
                        {
                            if (codeError != SocketError.ConnectionReset && codeError != SocketError.ConnectionRefused && codeError != SocketError.ConnectionAborted && codeError != SocketError.Disconnecting && codeError != SocketError.TimedOut && codeError != SocketError.Success)
                            {
                                disconnectReason = $"[Receive] Socket Error [{codeError.ToString()}]";
                            }

                            if (codeError == SocketError.TimedOut)
                                disconnectReason = "TimedOut";

                            break;
                        }
                    }

                    thisClient.bytesReceived += tmpRecv;
                }
                else
                {
                    if (!thisClient.headerReceived)
                    {
                        thisClient.headerReceived = true;

                        int packetSize = getPacketSize();

                        if (packetSize < packetHeaderSize || packetSize > maxPacketSize)
                        {
                            disconnectReason = $"Wrong packet info ({packetSize})";
                            break;
                        }
                        continue;
                    }
                    /// handle client <exception cref="d"
                    if (onNewPacketEv != null)
                        onNewPacketEv(rcvBuff, thisClient.clientId, thisClient.clientData);

                    thisClient.bytesReceived = 0;
                    thisClient.headerReceived = false;
                    packetsHandled++;
                }
            }

            on_disconnect:;

            onDisconnectEvent(thisClient, disconnectReason);
        }
        private void handleSocketWrite(client thisClient)
        {
            if (thisClient.sendStream.IsEmpty())
            {
                stopWriteClient(thisClient);
                return;
            }

            IntPtr buffHndl = thisClient.sendStream.FlushAll(out int bytesToSend);
            ReadOnlySpan<byte> dataBuffer = new ReadOnlySpan<byte>((void*)buffHndl, bytesToSend);

            int sended = thisClient.clientSock.Send(dataBuffer, SocketFlags.None, out SocketError errorCode);

            if (errorCode != SocketError.Success)
            {
                if (errorCode != SocketError.Shutdown && errorCode != SocketError.ConnectionReset && errorCode != SocketError.ConnectionRefused && errorCode != SocketError.ConnectionAborted)
                {
                    onDisconnectEvent(thisClient, $"[Send] Socket Error [{errorCode.ToString()}]");
                    return;
                }

                onDisconnectEvent(thisClient, "Connection reset. [Send]");
                return;
            }
            else if (sended != bytesToSend)
                thisClient.sendStream.Write(buffHndl + sended, bytesToSend - sended);
        }

        void worker()
        {
            serverStarted = true;

            do
            {
                listenSock = new Socket(listenIp.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                try
                {
                    listenSock.Bind(new IPEndPoint(listenIp, listenPort));
                    listenSock.Listen(backLog);
                    listenSock.Blocking = false;

                    if (OnServerStarted != null)
                        OnServerStarted(listenIp, listenPort,backLog, true, connRetryTimeout);
                    //callServerEvent($"Server has been started on [{listenIp}:{listenPort}]");
                    break;
                }
                catch
                {
                    listenSock.Close();
                    listenSock = null;

                    if (OnServerStarted != null)
                        OnServerStarted(listenIp, listenPort, backLog, false, connRetryTimeout);
                    Thread.Sleep(connRetryTimeout);
                    continue;
                }
            } while (serverStarted);

            SpinWait spinwait = new SpinWait();

            while (serverStarted)
            {
                while (commandsQueue.TryDequeue(out serverCommand currentCommand))
                {
                    handleCommand(currentCommand);
                }

                List<Socket> checkRead = new List<Socket>(this.toReadSockets);
                List<Socket> checkWrite = new List<Socket>(this.toWriteSockets);

                checkRead.Add(listenSock);
                Socket.Select(checkRead, checkWrite, null, serverTickMicro);
                int countToRead = checkRead.Count;
                int countToWrite = checkWrite.Count;

                for (int i = 0; i < countToRead; ++i)
                {
                    Socket currentSocket = checkRead[i];

                    if (currentSocket != listenSock)
                    {
                        client thisClient = clients[clientAssocList[currentSocket]];

                        handleSocketRead(thisClient);
                    }
                    else
                    {
                        while (true)
                        {
                            Socket newClient;

                            try
                            {
                                newClient = currentSocket.Accept();
                            }
                            catch (SocketException exp)
                            {
                                if (exp.SocketErrorCode == SocketError.WouldBlock)
                                    break;

                                throw exp;
                            }
                            catch (Exception e)
                            {
                                if (OnServerException != null)
                                    OnServerException(e);
                                //callServerEvent($"Error accept client [exception={e.ToString()}]");
                                break;
                            }

                            onConnectEvent(newClient);
                        }
                    }
                }

                for (int i = 0; i < countToWrite; ++i)
                {
                    Socket currentSocket = checkWrite[i];

                    if (!clientAssocList.TryGetValue(currentSocket, out int clientId))
                        continue;

                    client thisClient = clients[clientId];

                    handleSocketWrite(thisClient);
                }

                while (secondCommandsQueue.TryDequeue(out serverCommand cmd))
                    commandsQueue.Enqueue(cmd);

                for (int i = 0; i < 4; ++i)
                    spinwait.SpinOnce();
            }
        }

    }
}
