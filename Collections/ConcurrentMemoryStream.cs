using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WFast.Collections
{
    public unsafe class ConcurrentMemoryStream 
    {
        const int _controlInfoSize = sizeof(int);

        IntPtr _hndl;

        IntPtr _writeptr;
        IntPtr _readptr;
        IntPtr _endPtr;

        public ConcurrentMemoryStream(int _bufferSize)
        {
            _hndl = Marshal.AllocHGlobal(_bufferSize);

            _writeptr = _hndl;
            _readptr = _hndl;
            _endPtr = _hndl + _bufferSize;

            new Span<byte>((void*)_hndl, _bufferSize).Fill(0);

        }

        ~ConcurrentMemoryStream()
        {
            Console.WriteLine("~ConcurrentMemoryStream()");
            Marshal.FreeHGlobal(_hndl);
        }

        public void DoubleWrite(byte *bf_1, int bf_1Size, byte *bf_2, int bf_2Size)
        {
            SpinWait sw = new SpinWait();
            IntPtr head;

            int count_bytes = bf_1Size + bf_2Size;

            do
            {
                head = _writeptr;
                IntPtr rslt = head + _controlInfoSize + count_bytes;

                if ((long)rslt > (long)_endPtr)
                {
                    sw.SpinOnce();
                    Console.WriteLine("memstream_double: overflow [{0}] + [{1}] bytesInBuffer={2}", bf_1Size, bf_2Size, (long)_writeptr - (long)_hndl);
                    continue;
                }

                if (Interlocked.CompareExchange(ref _writeptr, rslt, head) == head)
                    break;

                sw.SpinOnce();
            } while (true);

            Buffer.MemoryCopy((byte*)bf_1, (byte*)(head + _controlInfoSize), bf_1Size, bf_1Size);
            Buffer.MemoryCopy((byte*)bf_2, (byte*)(head + _controlInfoSize + bf_1Size), bf_2Size, bf_2Size);
            Interlocked.MemoryBarrier();
            *(int*)head = (int)(count_bytes | 0x80000000);
        }
        public void Write(byte* buffer, int count_bytes)
        {
            SpinWait sw = new SpinWait();
            IntPtr head;

            do
            {
                head = _writeptr;
                IntPtr rslt = head + _controlInfoSize + count_bytes;

                if ((long)rslt > (long)_endPtr)
                {
                    sw.SpinOnce();
                    Console.WriteLine("memstream: overflow [{0}] bytesInBuffer={1}", count_bytes, (long)_writeptr - (long)_hndl);
                    continue;
                }

                if (Interlocked.CompareExchange(ref _writeptr, rslt, head) == head)
                    break;

                sw.SpinOnce();
            } while (true);


            Buffer.MemoryCopy(buffer,(byte*)(head + _controlInfoSize), count_bytes, count_bytes);
            Interlocked.MemoryBarrier();
            *(int*)head = (int)(count_bytes | 0x80000000);
            
           // return head;
        }
        public int Read(byte* _OutPut)
        {
            if (_writeptr == _readptr)
            {
                IntPtr _r = Interlocked.CompareExchange(ref _writeptr, _hndl, _readptr);

                if (_r == _readptr)
                {
                    _readptr = _hndl;
                    return 0;
                }
                else
                    return Read(_OutPut);
            }

            SpinWait sw = new SpinWait();
            int control_info;
            do
            {
                control_info = *(int*)_readptr;
                byte flag = (byte)((control_info & 0x80000000) >> 31);

                if (flag == 1)
                    break;

                sw.SpinOnce();
            } while (true);
            
            control_info &= 0x7fff;

            Buffer.MemoryCopy((byte*)(_readptr + _controlInfoSize), _OutPut, control_info, control_info);
            new Span<byte>((byte*)_readptr, control_info + _controlInfoSize).Fill(0);
            _readptr += control_info + _controlInfoSize;
            return control_info;
        }
    }
}
