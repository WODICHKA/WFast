using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace WFast.Networking.Protocol
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    public struct packetHeader
    {
        public int Size;
        public short Reserved;
    }

    public unsafe class Packet : IPacket
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CalculatePacketSize(byte* buffer) => *(int*)buffer;

        public readonly static int HeaderSize = Marshal.SizeOf<packetHeader>();

        private byte* buffPtr;
        private packetHeader _pHeader;
        private int _offset;

        public int Size { get { return _pHeader.Size; } }

        private Packet() { }
        public Packet(packetHeader header)
        {
            _pHeader = header;

            int packetSize = Size;

            if (packetSize < HeaderSize)
                throw new IndexOutOfRangeException();

            buffPtr = (byte*)Marshal.AllocHGlobal(packetSize);
            new Span<byte>(buffPtr, packetSize).Fill(0);

            *(packetHeader*)(buffPtr) = _pHeader;
            Seek(0, SeekOrigin.Begin);
        }
        public Packet(ReadOnlySpan<byte> bf)
        {
            if (bf.Length < HeaderSize)
                throw new IndexOutOfRangeException();

            int packetSize = 0;

            fixed (byte* pinnedBuffer = &MemoryMarshal.GetReference(bf))
                packetSize = CalculatePacketSize(pinnedBuffer);

            if (packetSize < HeaderSize)
                throw new IndexOutOfRangeException();

            buffPtr = (byte*)Marshal.AllocHGlobal(packetSize);

            fixed (byte* pinnedBuffer = &MemoryMarshal.GetReference(bf))
                Buffer.MemoryCopy(pinnedBuffer, buffPtr, packetSize, packetSize);

            _pHeader = *(packetHeader*)(buffPtr);
            Seek(0, SeekOrigin.Begin);
        }
        public Packet(byte[] bf)
        {
            if (bf.Length < HeaderSize)
                throw new IndexOutOfRangeException();

            int packetLenght = 0;

            fixed (byte* pinnedBuffer = bf)
                packetLenght = CalculatePacketSize(pinnedBuffer);

            if (packetLenght < HeaderSize)
                throw new IndexOutOfRangeException();

            buffPtr = (byte*)Marshal.AllocHGlobal(packetLenght);

            Marshal.Copy(bf, 0, (IntPtr)buffPtr, packetLenght);

            _pHeader = *(packetHeader*)(buffPtr);
            Seek(0, SeekOrigin.Begin);
        }
        ~Packet()
        {
            if (buffPtr != (byte*)IntPtr.Zero)
            {
                Marshal.FreeHGlobal((IntPtr)buffPtr);
                buffPtr = (byte*)IntPtr.Zero;
            }
        }

        public unsafe void WriteInt64(long _Val)
        {
            if (!canAccess(sizeof(long)))
                throw new OverflowException();

            *(long*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(long));
        }
        public unsafe void WriteUInt64(ulong _Val)
        {
            if (!canAccess(sizeof(ulong)))
                throw new OverflowException();

            *(ulong*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(ulong));
        }
        public unsafe void WriteInt32(int _Val)
        {
            if (!canAccess(sizeof(int)))
                throw new OverflowException();

            *(int*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(int));
        }
        public unsafe void WriteUInt32(uint _Val)
        {
            if (!canAccess(sizeof(uint)))
                throw new OverflowException();

            *(uint*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(uint));
        }
        public unsafe void WriteInt16(short _Val)
        {
            if (!canAccess(sizeof(short)))
                throw new OverflowException();

            *(short*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(short));
        }
        public unsafe void WriteUInt16(ushort _Val)
        {
            if (!canAccess(sizeof(ushort)))
                throw new OverflowException();

            *(ushort*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(ushort));
        }
        public unsafe void WriteByte(byte _Val)
        {
            if (!canAccess(sizeof(byte)))
                throw new OverflowException();

            *(byte*)(buffPtr + _offset) = _Val;
            forwardSeek(sizeof(byte));
        }
        public unsafe void WriteNullTerminateString(char[] chars, Encoding encode)
        {
            WriteString(chars, encode);
            WriteByte(0);
        }
        public unsafe void WriteString(char[] chars, Encoding encode)
        {
            int lenght = encode.GetByteCount(chars);

            if (!canAccess(lenght))
                throw new OverflowException();

            byte[] inputBytes = new byte[lenght];

            encode.GetBytes(chars, 0, chars.Length, inputBytes, 0);

            new ReadOnlySpan<byte>(inputBytes).CopyTo(new Span<byte>(buffPtr, lenght));

            forwardSeek(lenght);
        }
        public unsafe void WriteNullTerminateString(string str, Encoding encode)
        {
            WriteString(str, encode);
            WriteByte(0);
        }
        public unsafe void WriteString(string str, Encoding encode)
        {
            byte[] resultBuffer = encode.GetBytes(str);
            int lenght = resultBuffer.Length;

            if (!canAccess(lenght))
                throw new OverflowException();

            fixed (byte* pinnedBuffer = resultBuffer)
                Buffer.MemoryCopy(pinnedBuffer, buffPtr + _offset, lenght, lenght);

            forwardSeek(lenght);
        }
        public unsafe void WriteArrayOfBytes(ReadOnlySpan<byte> bytes)
        {
            int lenght = bytes.Length;

            if (!canAccess(lenght))
                throw new OverflowException();

            fixed (byte* pinnedSpan = &MemoryMarshal.GetReference(bytes))
                Buffer.MemoryCopy(pinnedSpan, buffPtr + _offset, lenght, lenght);

            forwardSeek(lenght);
        }

        public unsafe long ReadInt64()
        {
            if (!canAccess(sizeof(long)))
                throw new OverflowException();

            long result = *(long*)(buffPtr + _offset);
            forwardSeek(sizeof(long));
            return result;
        }
        public unsafe ulong ReadUInt64()
        {
            if (!canAccess(sizeof(ulong)))
                throw new OverflowException();

            ulong result = *(ulong*)(buffPtr + _offset);
            forwardSeek(sizeof(ulong));
            return result;
        }
        public unsafe int ReadInt32()
        {
            if (!canAccess(sizeof(int)))
                throw new OverflowException();

            int result = *(int*)(buffPtr + _offset);
            forwardSeek(sizeof(int));
            return result;
        }
        public unsafe uint ReadUInt32()
        {
            if (!canAccess(sizeof(uint)))
                throw new OverflowException();

            uint result = *(uint*)(buffPtr + _offset);
            forwardSeek(sizeof(uint));
            return result;
        }
        public unsafe short ReadInt16()
        {
            if (!canAccess(sizeof(short)))
                throw new OverflowException();

            short result = *(short*)(buffPtr + _offset);
            forwardSeek(sizeof(short));
            return result;
        }
        public unsafe ushort ReadUInt16()
        {
            if (!canAccess(sizeof(ushort)))
                throw new OverflowException();

            ushort result = *(ushort*)(buffPtr + _offset);
            forwardSeek(sizeof(ushort));
            return result;
        }
        public unsafe byte ReadByte()
        {
            if (!canAccess(sizeof(byte)))
                throw new OverflowException();

            byte result = *(byte*)(buffPtr + _offset);
            forwardSeek(sizeof(byte));
            return result;
        }
        public unsafe string ReadString(int lenght, Encoding encode)
        {
            if (!canAccess(lenght))
                throw new OverflowException();

            string resultString = encode.GetString(buffPtr + _offset, lenght);

            forwardSeek(lenght);
            return resultString;
        }
        public unsafe string ReadNullTerminateString(Encoding encode)
        {
            int lenght = findStringLenght();

            if (lenght == -1)
                throw new OverflowException();
            else if (lenght == -2)
                throw new ArgumentException();

            if (lenght > 0)
            {
                string resultString = encode.GetString(buffPtr + _offset, lenght);

                forwardSeek(lenght + 1);
                return resultString;
            }
            else
            {
                forwardSeek(1);
                return string.Empty;
            }
        }
        public unsafe ReadOnlySpan<byte> ReadBytesAsSpan(int cb)
        {
            if (!canAccess(cb))
                throw new OverflowException();

            ReadOnlySpan<byte> result = new ReadOnlySpan<byte>(buffPtr + _offset, cb);
            forwardSeek(cb);
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] ReadBytes(int cb)
        {
            return ReadBytesAsSpan(cb).ToArray();
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int findStringLenght()
        {
            if (_offset >= Size || _offset < 0)
                return -1;

            int start = _offset;

            for (int currentPos = _offset; currentPos < Size; ++currentPos)
            {
                byte thisByte = *(buffPtr + currentPos);
                if (thisByte == 0)
                    return currentPos - start;
            }

            return -2;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool canAccess(int cb)
        {
            int resultOffset = _offset + cb;

            if (resultOffset > Size || resultOffset < 0)
                return false;

            return true;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void forwardSeek(int off)
        {
            int resultOff = _offset + off;

            if (resultOff > Size)
                resultOff = Size;
            else if (resultOff < 0)
                resultOff = 0;

            _offset = resultOff;
        }

        public void Seek(int offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    {
                        int resultOffset = offset + HeaderSize;

                        if (resultOffset > Size || resultOffset < 0)
                            throw new ArgumentException($"offset overflow [{resultOffset} > {Size} || {resultOffset} < 0]", "offset");

                        _offset = resultOffset;
                    }
                    break;
                case SeekOrigin.End:
                    {
                        int resultOffset = Size - offset;

                        if (resultOffset > Size || resultOffset < 0)
                            throw new ArgumentException($"offset overflow [{resultOffset} > {Size} || {resultOffset} < 0]", "offset");

                        _offset = resultOffset;
                    }
                    break;
                case SeekOrigin.Current:
                    {
                        int resultOffset = _offset + offset;

                        if (resultOffset > Size || resultOffset < 0)
                            throw new ArgumentException($"offset overflow [{resultOffset} > {Size} || {resultOffset} < 0]", "offset");

                        _offset = resultOffset;
                    }
                    break;
                default:
                    throw new ArgumentException($"wrong origin: {origin.ToString()}", "origin");
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> GetByteSpan()
            => new ReadOnlySpan<byte>(buffPtr, _pHeader.Size);
    }
}
