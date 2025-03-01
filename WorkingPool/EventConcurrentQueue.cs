using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace WFast.Threading
{
    public unsafe class EventConcurrentQueue<T> where T : class
    {
        struct SToken
        {
            public int mReaderAwake;
            public int mWriterCnt;

            public static long Awake => 1;//new SToken() { mReaderAwake = 1, mWriterCnt = 0 };
            public static long Sleep => 0;// new SToken() { mWriterCnt = 0, mReaderAwake = 0 };
        }

        private long mToken;
        private IProducerConsumerCollection<T> mCollection;
        private ManualResetEventSlim mWaitHandler;

        public int Count => mCollection.Count;

        public EventConcurrentQueue(IProducerConsumerCollection<T> pCollection)
        {
            mCollection = pCollection;
            mWaitHandler = new ManualResetEventSlim(false);
            mToken = 0;
        }

        /// <summary>
        /// Return True If call kernel
        /// </summary>
        /// <returns></returns>
        public unsafe bool Set()
        {
            if (Volatile.Read(ref mToken) == SToken.Sleep)
            {
                mWaitHandler.Set();
                return true;
            }

            do
            {
                long aToken = Volatile.Read(ref mToken);
                long aPrevToken = aToken;

                if (aToken == SToken.Sleep)
                {
                    mWaitHandler.Set();
                    return true;
                }

                SToken* aNewToken_aToken = (SToken*)&aToken;
                aNewToken_aToken->mWriterCnt = aNewToken_aToken->mWriterCnt + 1;

                if (Interlocked.CompareExchange(ref mToken, aToken, aPrevToken) == aPrevToken)
                    return false;

            } while (true);
        }

        public void BeginWait(int aMillisecondTimeout = -1)
        {
            if (Volatile.Read(ref mToken) != SToken.Sleep)
                throw new Exception($"EventConcurrentQueue::BeginWait: Reader already awake. Allow only one Reader");

            mWaitHandler.Wait(aMillisecondTimeout);

            long aCurrentToken = Volatile.Read(ref mToken);
            long aAwakenToken = SToken.Awake;

            if (Interlocked.CompareExchange(ref mToken, aAwakenToken, aCurrentToken) != aCurrentToken)
                throw new Exception($"EventConcurrentQueue::BeginWait: In this queue you can have only one reader.");

            mWaitHandler.Reset();
        }
        /// <summary>
        /// Return True If No New Writers when Read Old
        /// </summary>
        /// <returns></returns>
        public bool EndWait(out bool pNewRound)
        {
            long aCurrentToken = Volatile.Read(ref mToken);

            if (aCurrentToken == SToken.Sleep)
                throw new Exception($"EventConcurrentQueue::EndWait: Reader incorrect state.");

            SToken* aCurrentSToken = (SToken*)&aCurrentToken;

            if (aCurrentSToken->mWriterCnt == 0)
            { // Пытаемся уснуть
                pNewRound = false;
                return Interlocked.CompareExchange(ref mToken, SToken.Sleep, aCurrentToken) == aCurrentToken;
            }
            else
            { // Пытаемся обнулить количество "Писателей" возвращаем в любом случае false для лишнего цикла
                Interlocked.CompareExchange(ref mToken, SToken.Awake, aCurrentToken);
                pNewRound = true;
                return false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryTake(out T Element) => mCollection.TryTake(out Element);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(T Element) => mCollection.TryAdd(Element);
    }
}
