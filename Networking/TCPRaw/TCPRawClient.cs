using PNGCService.Logs;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using WFast.Collections;
using WFast.Networking.Protocol;

namespace WFast.Networking
{
    public delegate void SocketEvent(IPAddress remote_ip, ushort remote_port, Exception e, string reason);
    public delegate void ClientEvent(byte[] buffer);
    public enum LatencyMode
    {
        Normal = 1,
        Slow = 2,
        Fast = 3,
        VeryFast = 4
    }
    public class TCPRawClient
    {
        private Thread clientThread;
        private int workerStarted;
        private IPAddress remoteIp;
        private ushort remotePort;
        private Socket remoteSocket;
        private LatencyMode mode;
        private PacketCalculateEvent CalculatePacketSize;

        private ConcurrentMemoryStream _outputStream;
        private int _maxPacketSize;

        public bool Connected { get; private set; }
        private bool setToDisconnect;

        private int _rcnTimeout;
        /// <summary>
        /// 100 < value < 60000
        /// </summary>
        public int ReconnectTimeout
        {
            get { return _rcnTimeout; }
            set
            {
                if (value < 100)
                    _rcnTimeout = 100;
                else if (value > 60000)
                    _rcnTimeout = 60000;
                else
                    _rcnTimeout = value;
            }
        }

        public event SocketEvent OnConnectSuccess;
        public event SocketEvent OnErrorConnect;
        public event SocketEvent OnDisconnect;
        public event ClientEvent OnNewPacket;

        private byte[] rcvBuff;

        private int bytesReceived;
        private bool headerReceived;
        private int packetHeaderSize;

        public TCPRawClient(IPAddress remote_ip, ushort remote_port, PacketCalculateEvent calc_event, int header_size, int maxPacketSize = 64 * 1024, int outputStreamMultiply = 64, LatencyMode mode = LatencyMode.Normal)
        {
            remoteIp = remote_ip;
            remotePort = remote_port;
            _maxPacketSize = maxPacketSize;
            this.mode = mode;

            if (calc_event == null)
                throw new ArgumentNullException(nameof(calc_event));

            packetHeaderSize = header_size;
            CalculatePacketSize = calc_event;
            workerStarted = 0;
            _outputStream = new ConcurrentMemoryStream(_maxPacketSize * outputStreamMultiply);
            rcvBuff = new byte[maxPacketSize];
        }
        public void Disconnect()
        {
            setToDisconnect = true;
        }

        public void SetLatencyMode(LatencyMode mode)
        {
            this.mode = mode;
        }
        public unsafe bool SendPacketNoWait(IPacket packet)
        {
            return _outputStream.WriteNoWait(packet.GetByteSpan());
        }
        public unsafe void SendPacket(IPacket packet)
        {
            _outputStream.Write(packet.GetByteSpan());
        }
        public unsafe void SendPacket(byte[] buffer, int index, int count_bytes)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (index < 0 || count_bytes <= 0)
                throw new ArgumentException(nameof(index) + "|" + nameof(count_bytes));

            if (buffer.Length < (index + count_bytes))
                throw new IndexOutOfRangeException(nameof(buffer));

            fixed (byte* pinnedBuffer = buffer)
                _outputStream.Write(pinnedBuffer + index, count_bytes);
        }

        public void StartWorker(bool inThisThread = false)
        {
            if (clientThread != null || Volatile.Read(ref workerStarted) == 1)
                return;

            if (inThisThread)
            {
                worker();
                return;
            }

            clientThread = new Thread(worker);
            clientThread.IsBackground = true;
            clientThread.Start();
        }

        private unsafe void handleWriteEvent(MemoryStream writeStream)
        {
            if (writeStream.IsEmpty())
                return;

            int tmpSend = remoteSocket.Send(writeStream.CanReadPtr(), SocketFlags.None, out SocketError errorCode);
            Logger.WriteLine($"Sended to [{this.remoteIp}:{this.remotePort}] {tmpSend} bytes");
            if (errorCode != SocketError.Success)
            {
                if (errorCode != SocketError.Shutdown && errorCode != SocketError.ConnectionReset && errorCode != SocketError.ConnectionRefused && errorCode != SocketError.ConnectionAborted)
                {
                    makeDisconnect($"[Send]: Socket error [{errorCode.ToString()}]");
                    return;
                }

                makeDisconnect($"[Send] Connection reset.");
                return;
            }

            writeStream.MarkAsRead(tmpSend);
        }
        private unsafe void handleReadEvent()
        {
            int getPacketSize()
            {
                fixed (byte* pinnedBuffer = rcvBuff)
                    return CalculatePacketSize(pinnedBuffer);
            }

            int needToRecv = 0;
            int packetsHandled = 0;
            int tmpRecv = 0;
            int bytesInBuffer = 0;
            SocketError codeError;

            string disconnectReason = "Connection reset.";

            bytesInBuffer = remoteSocket.Available;

            if (bytesInBuffer == 0)
            {
                goto on_disconnect;
            }

            for (; ; )
            {
                needToRecv = !headerReceived ?
                                (packetHeaderSize - bytesReceived) :
                                (getPacketSize() - bytesReceived);

                if (needToRecv != 0)
                {
                    tmpRecv = remoteSocket.Receive(rcvBuff, bytesReceived, needToRecv, SocketFlags.None, out codeError);

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

                    bytesReceived += tmpRecv;
                }
                else
                {
                    if (!headerReceived)
                    {
                        headerReceived = true;

                        int packetSize = getPacketSize();

                        if (packetSize < packetHeaderSize || packetSize > _maxPacketSize)
                        {
                            disconnectReason = $"Wrong packet info ({packetSize})";
                            break;
                        }
                        continue;
                    }
                    /// handle client <exception cref="d"
                    if (OnNewPacket != null)
                        OnNewPacket(rcvBuff);

                    bytesReceived = 0;
                    headerReceived = false;
                    packetsHandled++;
                }
            }

            on_disconnect:;
            makeDisconnect(disconnectReason);
        }
        private unsafe void makeDisconnect(string reason)
        {
            _outputStream.Clear();
            destroySocket();

            if (OnDisconnect != null)
                OnDisconnect(remoteIp, remotePort, null, reason);
        }
        private unsafe void worker()
        {
            if (Interlocked.CompareExchange(ref workerStarted, 1, 0) == 1)
                return;

            MemoryStream localStream = new MemoryStream((IntPtr)(_maxPacketSize * 2), true);
            byte* buffer = (byte*)Marshal.AllocHGlobal(_maxPacketSize);
            int readed = 0;

            int msDelay = 0;
            Connected = false;
            try
            {
                while (Volatile.Read(ref workerStarted) == 1)
                {
                    if (remoteSocket == null)
                    {
                        localStream.Clear();
                        while (!Connect())
                            Thread.Sleep(ReconnectTimeout);
                    }

                    if (setToDisconnect)
                    {
                        makeDisconnect("called");
                        continue;
                    }

                    List<Socket> toRead = new List<Socket>();
                    List<Socket> toWrite = new List<Socket>();

                    toRead.Add(remoteSocket);

                    if (!_outputStream.IsEmpty())
                    {
                        toWrite.Add(remoteSocket);

                        do
                        {
                            readed = _outputStream.Read(buffer);

                            if (readed == 0)
                                break;

                            localStream.Write(new ReadOnlySpan<byte>(buffer, readed));
                        } while (!_outputStream.IsEmpty());
                    }

                    if (mode == LatencyMode.Normal)
                        msDelay = 5000;
                    else if (mode == LatencyMode.Fast)
                        msDelay = 500;
                    else if (mode == LatencyMode.VeryFast)
                        msDelay = 50;
                    else if (mode == LatencyMode.Slow)
                        msDelay = 25000;
                    else
                        msDelay = 5000;

                    Socket.Select(toRead, toWrite, null, msDelay);

                    if (toRead.Count > 0)
                        handleReadEvent();

                    if (remoteSocket == null)
                        continue;

                    if (toWrite.Count > 0)
                        handleWriteEvent(localStream);
                }
            }
            catch (Exception e) { throw e; }
            finally
            {
                Marshal.FreeHGlobal((IntPtr)buffer);
            }
        }

        private void destroySocket()
        {
            if (remoteSocket != null)
            {
                remoteSocket.Close();
                remoteSocket = null;
            }
        }

        private bool Connect(bool connectUntilSuccess = false)
        {
            if (remoteSocket != null)
                return true;

            retry:;

            try
            {
                remoteSocket = new Socket(remoteIp.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                remoteSocket.Connect(remoteIp, remotePort);

                remoteSocket.Blocking = false;

                if (OnConnectSuccess != null)
                    OnConnectSuccess(remoteIp, remotePort, null, "");

                Connected = true;
                setToDisconnect = false;
                return true;
            }
            catch (Exception e)
            {
                destroySocket();

                if (OnErrorConnect != null)
                    OnErrorConnect(remoteIp, remotePort, e, "");

                Connected = false;

                if (connectUntilSuccess)
                    goto retry;

                return false;
            }
        }
    }
}