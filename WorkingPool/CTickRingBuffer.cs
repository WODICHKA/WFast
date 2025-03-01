using System;
using System.Collections.Generic;
using System.Linq;

namespace WFast.Threading
{
    public class CTickRingBuffer<T> where T : class
    {
        private readonly int mTickrate;
        private LinkedList<List<T>> mList;

        public long LastProcessTick;
        public int Count { get; private set; }

        public CTickRingBuffer(long pInitTick, int pTick = 64)
        {
            mTickrate = pTick;
            mList = new LinkedList<List<T>>();

            for (int i = 0; i < pTick; ++i)
                mList.AddLast(new List<T>(32));
        }

        public void ProcessItemsForTick(long pNowTick, long pRealTick, Action<long, T> pProcessor)
        {
            if (pNowTick >= LastProcessTick + mTickrate)
            { // Читаем все что есть
                foreach (var aList in mList)
                {
                    for (int j = 0; j < aList.Count; ++j)
                        pProcessor.Invoke(pRealTick, aList[j]);

                    if (aList.Count > 0)
                        aList.Clear();
                }

                Count = 0;
                LastProcessTick = pNowTick;
            }
            else
            {
                for (; LastProcessTick < pNowTick; ++LastProcessTick)
                {
                    if (Count > 0)
                    {
                        var aFirstList = mList.First.Value;

                        mList.Remove(mList.First);

                        if (aFirstList.Count > 0)
                        {
                            for (int j = 0; j < aFirstList.Count; ++j)
                                pProcessor.Invoke(pRealTick, aFirstList[j]);

                            Count -= aFirstList.Count;
                            aFirstList.Clear();
                        }

                        mList.AddLast(aFirstList);
                    }
                }
            }

            return;
        }

        public void Add(T pItem, long pTick)
        {
            if (pTick <= LastProcessTick)
                throw new Exception($"CTickRingBuffer::Add({pTick}): Tick already processed. LastProcessTick={LastProcessTick} ({LastProcessTick - pTick})");
            if (pTick > (LastProcessTick + mTickrate))
                throw new Exception($"CTickRingBuffer::Add({pTick}): Tick value too big. LastProcessTick={LastProcessTick} ({LastProcessTick - pTick})");

            // LastProcessTick = 1
            // pTick = 60
            // index 0, tick 2 (pTick - LastProc (1)) - 1
            // index 59, tick (pTick - LastProc (59))
            int aIndex = (int)(pTick - LastProcessTick) - 1;

            mList.ElementAt(aIndex).Add(pItem);
            Count++;
        }
    }
}