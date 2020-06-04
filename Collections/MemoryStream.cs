using System;
using System.Runtime.InteropServices;

namespace WFast.Collections
{
    public class MemoryStreamIsFull : Exception
    {
        public MemoryStreamIsFull(int thisBuffer, long bytesInBuffer, long bufferSize) : base($"MemoryStreamIsFull: this_buffer={thisBuffer}, buffer=[{bytesInBuffer}/{bufferSize}]") { }
    }
    public class MemoryStreamErrorGrow : Exception
    {
        public MemoryStreamErrorGrow(int thisBuffer, long bytesInBuffer, long bufferSize, long maxBufferSize) : base($"MemoryStreamErrorGrow: this_buffer={thisBuffer}, buffer=[{bytesInBuffer}/{bufferSize}], max_buffer_size={maxBufferSize}") { }
    }
    public class MemoryStreamIsEmpty : Exception
    {
        public MemoryStreamIsEmpty() : base($"MemoryStreamIsEmpty") { }
    }

    public class MemoryStream
    {
        private IntPtr _handle;

        private IntPtr _readPtr;
        private IntPtr _writePtr;

        private IntPtr _endPtr;

        private bool _canGrow;

        private long _bufferSize;
        private long _bytesInBuffer;
        private long _maxBufferSize;

        public MemoryStream(IntPtr stream_size, bool can_grow = false, long maxBufferSize = -1)
        {
            _handle = Marshal.AllocHGlobal(stream_size);

            _readPtr = _handle;
            _writePtr = _handle;
            _maxBufferSize = maxBufferSize;

            _canGrow = can_grow;

            _endPtr = (IntPtr)(_handle.ToInt64() + stream_size.ToInt64());
            _bytesInBuffer = 0;
            _bufferSize = stream_size.ToInt64();
        }

        ~MemoryStream()
        {
            if (_handle != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_handle);
                _handle = IntPtr.Zero;
            }
        }

        public bool IsEmpty() => _writePtr == _readPtr;

        private unsafe bool grow(long need_size)
        {
            long resultNewSize = _bufferSize * 2;

            while (resultNewSize < need_size)
                resultNewSize *= 2;

            if (_maxBufferSize != -1 && resultNewSize > _maxBufferSize)
            {
                resultNewSize = _maxBufferSize;

                if (resultNewSize < need_size)
                    return false;
            }

            IntPtr newHandle = Marshal.AllocHGlobal((IntPtr)resultNewSize);

            try
            {
                Buffer.MemoryCopy((void*)_handle, (void*)newHandle, resultNewSize, _bufferSize);
            }
            catch (Exception e) { Marshal.FreeHGlobal(newHandle); throw e; }

            IntPtr writeOff = (IntPtr)(_writePtr.ToInt64() - _handle.ToInt64());
            IntPtr readOff = (IntPtr)(_readPtr.ToInt64() - _handle.ToInt64());

            _bufferSize = resultNewSize;

            if (_handle != IntPtr.Zero)
                Marshal.FreeHGlobal(_handle);

            _handle = newHandle;

            _writePtr = (IntPtr)(newHandle.ToInt64() + writeOff.ToInt64());
            _readPtr = (IntPtr)(newHandle.ToInt64() + readOff.ToInt64());
            _endPtr = (IntPtr)(newHandle.ToInt64() + resultNewSize);

            return true;
        }
        public unsafe Span<byte> CanReadPtr()
        {
            if (_bytesInBuffer == 0)
                return new Span<byte>();

            int bytes_in_buff = 0;

            if (_bytesInBuffer > int.MaxValue)
                bytes_in_buff = int.MaxValue;
            else
                bytes_in_buff = checked((int)_bytesInBuffer);

            return new Span<byte>((void*)_readPtr, bytes_in_buff);
        }
        public void MarkAsRead(int cb)
        {
            if (_bytesInBuffer == 0)
                throw new MemoryStreamIsEmpty();

            if (cb <= 0)
                throw new ArgumentException(nameof(cb));

            if (cb > _bytesInBuffer)
                throw new ArgumentOutOfRangeException(nameof(cb));

            _readPtr = IntPtr.Add(_readPtr, cb);
            _bytesInBuffer -= cb;

            if (_readPtr == _writePtr)
            {
                _readPtr = _handle;
                _writePtr = _handle;
            }
        }

        public unsafe void Write(ReadOnlySpan<byte> bytes)
        {
            int countBytes = bytes.Length;

            if (countBytes == 0)
                return;

            IntPtr resultPtr = IntPtr.Add(_writePtr, countBytes);

            if (resultPtr.ToInt64() > _endPtr.ToInt64())
            {
                if (!_canGrow)
                    throw new MemoryStreamIsFull(countBytes, _bytesInBuffer, _bufferSize);

                if (!grow(_bytesInBuffer + countBytes))
                    throw new MemoryStreamErrorGrow(countBytes, _bytesInBuffer, _bufferSize, _maxBufferSize);

                Write(bytes);
                return;
            }

            fixed (byte* pinnedBuffer = &MemoryMarshal.GetReference(bytes))
                Buffer.MemoryCopy(pinnedBuffer, (void*)_writePtr, _bufferSize - _bytesInBuffer, countBytes);

            _bytesInBuffer += countBytes;
            _writePtr = resultPtr;
        }
    }
}
