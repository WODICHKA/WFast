using System;
using System.Runtime.InteropServices;

namespace WFast.Collections
{
    public class MemoryStreamIsFullException : Exception
    {
        public MemoryStreamIsFullException(int bufferSize, int tryWriteBuffer, int controlSize, string method) : base ($"[MemoryStream].[{method}]: memoryStream is full. BufferSize: [{bufferSize}], TryWriteBuffer: [{tryWriteBuffer}], ControlInfoSize: [{controlSize}]")
        { }
    }

    public unsafe class MemoryStream
    {
        [StructLayout(LayoutKind.Sequential, Pack = 1)]
        internal struct _controlInfo
        {
            public int size;
            public int reserved;
        }

        private IntPtr _head;

        private IntPtr _writePtr;
        private int controlBufferSize;
        private IntPtr _endPtr;

        private void setToStart()
        {
            _writePtr = _head;
        }

        public MemoryStream(int bufferSize)
        {
            _head = Marshal.AllocHGlobal(bufferSize);
            setToStart();
            _endPtr = _head + bufferSize;
            controlBufferSize = Marshal.SizeOf<_controlInfo>();
        }

        internal static unsafe void memcpyimpl(byte* src, byte* dest, int len)
        {
            if (len == 1)
                *dest = *src;
            else if ((len & 0x01) != 0)
            {
                ___memcpyimpl(src, dest, len - 1);
                *(dest + (len - 1)) = *(src + (len - 1));
            }
            else
                ___memcpyimpl(src, dest, len);
        }
        internal static unsafe void zeromemoryimpl(byte* dest, int len)
        {
            if (len == 1)
                *dest = 0;
            else if ((len & 0x01) != 0)
            {
                ___zeroMemory(dest, len - 1);
                *(dest + (len - 1)) = 0;
            }
            else
                ___zeroMemory(dest, len);
        }
        internal static unsafe void ___zeroMemory(byte* dest, int len)
        {
            if (len >= 0x10)
            {
                do
                {
                    *((long*)dest) = 0;
                    *((long*)(dest + 8)) = 0;
                    dest += 0x10;
                }
                while ((len -= 0x10) >= 0x10);
            }
            if (len > 0)
            {
                if ((len & 8) != 0)
                {
                    *((long*)dest) = 0;
                    dest += 8;
                }
                if ((len & 4) != 0)
                {
                    *((int*)dest) = 0;
                    dest += 4;
                }
                if ((len & 2) != 0)
                {
                    *((short*)dest) = 0;
                    dest += 2;
                }
                if ((len & 1) != 0)
                {
                    dest++;
                    dest[0] = 0;
                }
            }
        }
        internal static unsafe void ___memcpyimpl(byte* src, byte* dest, int len)
        {
            if (len >= 0x10)
            {
                do
                {
                    *((long*)dest) = *((long*)src);
                    //*((int*)(dest + 4)) = *((int*)(src + 4));
                    *((long*)(dest + 8)) = *((long*)(src + 8));
                    // *((int*)(dest + 12)) = *((int*)(src + 12));
                    dest += 0x10;
                    src += 0x10;
                }
                while ((len -= 0x10) >= 0x10);
            }
            if (len > 0)
            {
                if ((len & 8) != 0)
                {
                    *((long*)dest) = *((long*)src);
                    //    *((int*)(dest + 4)) = *((int*)(src + 4));
                    dest += 8;
                    src += 8;
                }
                if ((len & 4) != 0)
                {
                    *((int*)dest) = *((int*)src);
                    dest += 4;
                    src += 4;
                }
                if ((len & 2) != 0)
                {
                    *((short*)dest) = *((short*)src);
                    dest += 2;
                    src += 2;
                }
                if ((len & 1) != 0)
                {
                    dest++;
                    src++;
                    dest[0] = src[0];
                }
            }
        }

        public void Write(IntPtr source, int cb)
        {
            IntPtr resultWritePtr = _writePtr + cb;

            if (resultWritePtr.ToInt64() > _endPtr.ToInt64() || resultWritePtr.ToInt64() < _head.ToInt64())
                throw new MemoryStreamIsFullException((int)(this._endPtr.ToInt64() - this._head.ToInt64()), cb, controlBufferSize, "Write(IntPtr, int32)");

            memcpyimpl((byte*)source,(byte*)(_writePtr), cb);

            _writePtr = resultWritePtr;
        }
        ~MemoryStream()
        {
            if (_head != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(_head);
                _head = IntPtr.Zero;
            }
        }

        public bool IsEmpty() => _writePtr == _head;

        public IntPtr FlushAll(out int bytesInBuffer)
        {
            bytesInBuffer = (int) (_writePtr.ToInt64() - _head.ToInt64());
            setToStart();

            return _head;
        }

    }
}
