
global using PoolTime = long;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;


namespace WFast.Threading
{
    /// <summary>
    /// T is must be enum int of power 2
    /// 1,2,4,8,16 ...
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SyncWorkerPool<T> where T : Enum
    {
        public class CPoolWorkerConfig
        {
            public T[] mHandlers;
            public int mCountThreads;
            public bool mSkipPoolMetric;
        }
        class CWorkerList
        {
            private int mAccessIndex;

            public readonly int[] mHandlers;
            public readonly CWorker[] mWorkers;
            public readonly int mCountWorkers;

            private int _mIndex;

            public CWorkerList(int pCountWorkers, int[] pHandlers)
            {
                if (pCountWorkers <= 0)
                    throw new ArgumentException($"CWorkerList({pCountWorkers}, [{string.Join('|', pHandlers)}]): wrong arguments");

                mHandlers = pHandlers;
                mCountWorkers = pCountWorkers;
                mWorkers = new CWorker[pCountWorkers];

                mAccessIndex = -1;
                _mIndex = 0;
            }

            public void CheckDeadlock(Action<string> pLogger)
            {
                for (int i = 0; i < mCountWorkers; ++i)
                    mWorkers[i].CheckDeadlock(pLogger);
            }
            public void SetCollectingMetrics()
            {
                for (int i = 0; i < mCountWorkers; ++i)
                    mWorkers[i].mMetricsIsEnabled = true;
            }
            public void SetDeadlockTimeout(int pTimeout)
            {
                for (int i = 0; i < mCountWorkers; ++i)
                    mWorkers[i].SetDeadlockTimeout(pTimeout);
            }

            public CWorker GetWorkerForExecuteQueue() => mCountWorkers > 0 ? mWorkers[Interlocked.Increment(ref mAccessIndex) % mCountWorkers] : mWorkers[0];

            public void AddWorker(CWorker pWorker)
            {
                mWorkers[_mIndex++] = pWorker;
            }
        }
        class CWorker
        {
            public CConfigureWorker mConfigure;

            public readonly Thread mThread;
            public readonly int mTID;
            public int mPID;

            public int mDeadlockTimeout;
            public SPoolQuery mProcessingQuery;
            public PoolTime mLastQueryStartTick;

            public bool mMetricsIsEnabled;
            public Stopwatch mMetricsSW;
            public CWorkerMetrics mMetrics;
            public Dictionary<MethodInfo, string> mMethodInfoCache;
            public bool mSkipPoolMetric;

            public CWorker(Thread pTh)
            {
                mThread = pTh;
                mTID = pTh.ManagedThreadId;
                mMetricsSW = new Stopwatch();
                mSkipPoolMetric = false;
                mMetricsIsEnabled = false;
                mMetrics = new CWorkerMetrics();
                mMethodInfoCache = new Dictionary<MethodInfo, string>();
            }
            public void ProcessQuery(SyncWorkerPool<T> _this, SPoolQuery aNewQuery)
            {
                bool aMetricsFlag = false;

                if (!aNewQuery._IsServiceQuery())
                {
                    if (mMetricsIsEnabled)
                    {
                        mMetricsSW.Restart();
                        aMetricsFlag = true;
                    }

                    aNewQuery.Process();

                    if (aMetricsFlag && mMetricsIsEnabled)
                    {
                        mMetricsSW.Stop();
                        mMetrics.PushMetric(aNewQuery.mHandler, aNewQuery.GetMethodInfo(mMethodInfoCache), mMetricsSW.ElapsedMilliseconds, mMetricsSW.ElapsedTicks);
                    }
                }
                else
                {
                    ProcessServiceQuery(_this, aNewQuery);
                }

                if (aNewQuery.mSync)
                    aNewQuery.Set();
                else
                    _this.PushQueryToFactory(aNewQuery);
            }
            public void ProcessServiceQuery(SyncWorkerPool<T> _this, SPoolQuery pQuery)
            {
                try
                {
                    pQuery.Validate();

                    switch (pQuery.mState)
                    {
                        case SPoolQuery.EPoolQueryState.ECollectMetrics:
                            {
                                _this.mOnWorkerMetrics?.Invoke(mMetrics.Collect(), $"{mThread.Name} (TID: {mTID} | PID: {mPID})");
                                mMetricsIsEnabled = false;
                                return;
                            }
                    }
                }
                catch (Exception e)
                {
                    pQuery.SetException(e);
                }
            }

            public void SetDeadlockTimeout(int pTimeout)
            {
                if (pTimeout <= 0)
                    mDeadlockTimeout = -1;
                else
                    mDeadlockTimeout = pTimeout;
            }
            public bool CheckDeadlock(Action<string> pLogger)
            {
                if (mDeadlockTimeout <= 0)
                    return false;

                PoolTime lastQuerySTick = mLastQueryStartTick;

                if (lastQuerySTick == 0)
                    return false;

                PoolTime msElapsed = Now() - lastQuerySTick;

                if (msElapsed >= mDeadlockTimeout)
                {
                    var thState = mThread.ThreadState;
                    var thResult = thState & System.Threading.ThreadState.WaitSleepJoin;

                    string aCurrentQueryInfo = CollectQueryInfo();

                    if (thResult == System.Threading.ThreadState.WaitSleepJoin)
                    {
                        try
                        {
                            pLogger?.Invoke($"Thread({mThread.ManagedThreadId})::Query={aCurrentQueryInfo} is possible to deadlock trying to interrupt...");

                            mThread.Interrupt();
                        }
                        catch (Exception e)
                        {
                            pLogger?.Invoke($"Thread({mThread.ManagedThreadId})::Query={aCurrentQueryInfo} exception then interrupting [{e.ToString()}]");
                        }
                        finally
                        {
                            mLastQueryStartTick = 0;
                        }

                        return true;
                    }
                    else
                    {
                        pLogger?.Invoke($"Thread({mThread.ManagedThreadId})::Query={aCurrentQueryInfo} too long making query ({msElapsed} ms)...");
                    }
                }
                return false;

                string CollectQueryInfo()
                {
                    string aCurrentQueryInfo = "(null)";

                    SPoolQuery aCurrentQuery = mProcessingQuery;

                    if (aCurrentQuery != null)
                        aCurrentQueryInfo = aCurrentQuery.ToString();
                    return aCurrentQueryInfo;
                }
            }

            public override bool Equals(object obj)
            {
                if (obj == null)
                    return false;
                if (obj is CWorker _Dst && _Dst.mConfigure.mID == this.mConfigure.mID)
                    return true;
                return false;
            }
            public override int GetHashCode()
            {
                return mConfigure.mID.GetHashCode();
            }
        }
        class CConfigureWorker
        {
            public int mID;
            public int[] mHandlers;
            public EventConcurrentQueue<SPoolQuery> mQueue;
        }
        class SPoolQuery
        {
            public enum EPoolQueryState : byte
            {
                ENone,
                ENonRetAct,
                ENonRetParamAct,
                ERetFunc,
                ERetParamFunc,
                EException,
                ECollectMetrics
            }

            static int _QueryUID;

            public readonly int mUID;
            public T mHandler { get; private set; }
            public PoolTime mTick { get; private set; }
            public int mTargetWorkerID { get; set; }

            public EPoolQueryState mState { get; private set; }
            private object mDelegate;
            private object mData;
            private object mResult;

            public bool mSync { get; private set; }
            private ManualResetEventSlim mWaitHandle;
            private bool mHandleSetted;

            public SPoolQuery()
            {
                mUID = Interlocked.Increment(ref _QueryUID);
                Reset();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CanExecuteNow() => mTick <= 0 || mTick <= Now();

            public void SetResult(object pResult)
            {
                mResult = pResult;
            }
            public SPoolQuery MakeNonRetAct(Action pNonRetAct, T pHandler, PoolTime pTick, bool pSync)
            {
                if (pNonRetAct == null) throw new Exception("SPoolQuery::MakeNonRetAct: delegate is null");

                mState = EPoolQueryState.ENonRetAct;
                mHandler = pHandler;
                mData = null;
                mDelegate = pNonRetAct;
                mTick = pTick;
                mSync = pSync;

                return this;
            }
            public SPoolQuery MakeNonRetParamAct(Action<object> pNonRetParamAct, object pData, T pHandler, PoolTime pTick, bool pSync)
            {
                if (pNonRetParamAct == null) throw new Exception("SPoolQuery::MakeNonRetParamAct: delegate is null");

                mState = EPoolQueryState.ENonRetParamAct;
                mHandler = pHandler;
                mDelegate = pNonRetParamAct;
                mData = pData;
                mTick = pTick;
                mSync = pSync;

                return this;
            }
            public SPoolQuery MakeRetFunc(Func<object> pRetFunc, T pHandler, PoolTime pTick, bool pSync)
            {
                if (pRetFunc == null) throw new Exception("SPoolQuery::MakeRetFunc: delegate is null");

                mState = EPoolQueryState.ERetFunc;
                mHandler = pHandler;
                mData = null;
                mDelegate = pRetFunc;
                mTick = pTick;
                mSync = pSync;

                return this;
            }
            public SPoolQuery MakeRetParamFunc(Func<object, object> pRetParamFunc, object pData, T pHandler, PoolTime pTick, bool pSync)
            {
                if (pRetParamFunc == null) throw new Exception("SPoolQuery::MakeRetParamFunc: delegate is null");

                mState = EPoolQueryState.ERetParamFunc;
                mHandler = pHandler;
                mDelegate = pRetParamFunc;
                mData = pData;
                mTick = pTick;
                mSync = pSync;

                return this;
            }
            public SPoolQuery _ServiceMakeCollectMetric(T pHandler)
            {
                mHandler = pHandler;
                mData = null;
                mState = EPoolQueryState.ECollectMetrics;
                mSync = false;
                mTick = -1;

                return this;
            }

            public bool _IsServiceQuery() => mState == EPoolQueryState.ECollectMetrics;

            public void SetException(Exception pExp)
            {
                mState = EPoolQueryState.EException;
                mResult = pExp;
            }
            public void Process()
            {
                try
                {
                    Validate();

                    switch (mState)
                    {
                        case EPoolQueryState.ENonRetAct:
                            {
                                ((Action)(mDelegate)).Invoke();
                                return;
                            }
                        case EPoolQueryState.ENonRetParamAct:
                            {
                                ((Action<object>)(mDelegate)).Invoke(mData);
                                return;
                            }
                        case EPoolQueryState.ERetFunc:
                            {
                                mResult = ((Func<object>)(mDelegate)).Invoke();
                                return;
                            }
                        case EPoolQueryState.ERetParamFunc:
                            {
                                mResult = ((Func<object, object>)(mDelegate)).Invoke(mData);
                                return;
                            }
                    }
                }
                catch (Exception e)
                {
                    SetException(e);
                }
            }
            public object Wait(int pLimitQuerySyncWait = -1)
            {
                if (mSync)
                {
                    mWaitHandle.Wait(pLimitQuerySyncWait);

                    if (mState != EPoolQueryState.EException)
                        return mResult;
                    else
                        throw (mResult as Exception);
                }
                else
                    return null;
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Validate()
            {
                if (mState == EPoolQueryState.ENone)
                    throw new Exception("SyncWorkerPool::SPoolQuery::Validate: query is disposed");
            }
            public void Set()
            {
                mHandleSetted = true;
                mWaitHandle.Set();
            }
            public void Reset()
            {
                mState = EPoolQueryState.ENone;
                mTargetWorkerID = -1;

                if (mWaitHandle == null)
                {
                    mWaitHandle = new ManualResetEventSlim(false);
                    mHandleSetted = false;
                }
                else
                {
                    if (mHandleSetted)
                    {
                        mWaitHandle.Reset();
                        mHandleSetted = false;
                    }
                }
            }

            public string GetMethodInfo(Dictionary<MethodInfo, string> aMethodInfoCache = null)
            {
                string mDelegateData;

                try
                {
                    MethodInfo aMethodInfo = null;

                    if (mState == EPoolQueryState.ENonRetAct)
                    {
                        if (mDelegate is Action aNonRetAct)
                            aMethodInfo = aNonRetAct.Method;
                    }
                    else if (mState == EPoolQueryState.ENonRetParamAct)
                    {
                        if (mDelegate is Action<object> aNonRetParamAct)
                            aMethodInfo = aNonRetParamAct.Method;
                    }
                    else if (mState == EPoolQueryState.ERetFunc)
                    {
                        if (mDelegate is Func<object> aRetFunc)
                            aMethodInfo = aRetFunc.Method;
                    }
                    else if (mState == EPoolQueryState.ERetParamFunc)
                    {
                        if (mDelegate is Func<object, object> aRetParamFunc)
                            aMethodInfo = aRetParamFunc.Method;
                    }
                    else
                        aMethodInfo = null;

                    if (aMethodInfo != null)
                    {
                        if (aMethodInfoCache == null || !aMethodInfoCache.TryGetValue(aMethodInfo, out mDelegateData))
                        {
                            Type? aReflectedType = aMethodInfo.ReflectedType;

                            string aParamInfo = "";
                            var aParamInfoObj = aMethodInfo.GetParameters();

                            for (int i = 0; i < aParamInfoObj.Length; ++i)
                                aParamInfo += $"{(aParamInfoObj[i].Name == null ? (null) : aParamInfoObj[i].ParameterType.FullName)}" + (i != (aParamInfoObj.Length - 1) ? "," : "");

                            if (aReflectedType != null)
                            {
                                mDelegateData = $"{aMethodInfo.ReturnType.Name} {aReflectedType.Name}.{aMethodInfo.Name} ({aParamInfo})";
                            }
                            else
                                mDelegateData = $"{aMethodInfo.ReturnType.Name} (null).{aMethodInfo.Name} ({aParamInfo})";

                            if (aMethodInfoCache != null)
                                aMethodInfoCache.Add(aMethodInfo, mDelegateData);
                        }
                    }
                    else
                        mDelegateData = "_WRONG_STATE_";
                }
                catch
                {
                    mDelegateData = "_EXCEPTION_";
                }

                return mDelegateData;
            }
            public string GetData()
            {
                string mDataData;

                try
                {
                    mDataData = (mData == null ? "null" : mData.ToString());
                }
                catch
                {
                    mDataData = "_EXCEPTION_";
                }
                return mDataData;
            }

            public override bool Equals(object obj)
            {
                if (obj == null)
                    return false;
                if (obj is SPoolQuery aPoll && aPoll.mUID == mUID)
                    return true;
                return false;
            }
            public override int GetHashCode()
            {
                return mUID.GetHashCode();
            }
            public override string ToString()
            {
                string mDelegateData = GetMethodInfo();
                string mDataData = GetData();

                return $"SPoolQuery.{mHandler} [mTick={mTick}, mSync={mSync}, mUID={mUID}, mState={mState}]: Delegate({mDelegateData}) Data({mDataData})";
            }
        }
        class IQueryComparer : IComparer<SPoolQuery>
        {
            public int Compare(SPoolQuery x, SPoolQuery y)
            {
                if (x.mTick < y.mTick)
                    return -1;
                else if (x.mTick > y.mTick)
                    return 1;
                else
                    return 0;
            }
        }

        public class CWorkerMetrics
        {
            public class CMethodMetrics
            {
                public long mTotalMS;
                public long mTotalTicks;
                public long mProcs;
            }
            public Dictionary<T, Dictionary<string, CMethodMetrics>> mHandlersMetrics;

            public void PushMetric(T pHandler, string pMethod, long pElapsedMS, long pElapsedTicks)
            {
                if (mHandlersMetrics == null)
                    mHandlersMetrics = new Dictionary<T, Dictionary<string, CMethodMetrics>>();

                if (mHandlersMetrics.TryGetValue(pHandler, out Dictionary<string, CMethodMetrics> aQueryesMetrics))
                {
                    if (aQueryesMetrics.TryGetValue(pMethod, out CMethodMetrics aMethodMetrics))
                    {
                        aMethodMetrics.mTotalMS += pElapsedMS;
                        aMethodMetrics.mTotalTicks += pElapsedTicks;
                        aMethodMetrics.mProcs++;
                    }
                    else
                        aQueryesMetrics.Add(pMethod, new CMethodMetrics() { mTotalMS = pElapsedMS, mTotalTicks = pElapsedTicks, mProcs = 1 });
                }
                else
                {
                    Dictionary<string, CMethodMetrics> aNewQueryesMetrics = new Dictionary<string, CMethodMetrics>();

                    aNewQueryesMetrics.Add(pMethod, new CMethodMetrics() { mTotalMS = pElapsedMS, mTotalTicks = pElapsedTicks, mProcs = 1 });
                    mHandlersMetrics.Add(pHandler, aNewQueryesMetrics);
                }
            }
            public Dictionary<T, Dictionary<string, CMethodMetrics>> Collect()
            {
                if (mHandlersMetrics != null)
                {
                    var aTmp = mHandlersMetrics;
                    mHandlersMetrics = null;
                    return aTmp;
                }
                else
                {
                    return new Dictionary<T, Dictionary<string, CMethodMetrics>>();
                }
            }
        }
        public class CPoolMetrics
        {
            public long mSchedulerDelayTicks;
            public long mSchedulerDelayTicksCnt;

            public long mSchedulerFasterTicks;
            public long mSchedulerFasterTicksCnt;

            public long mSchedulerPerfectHit;
            public long mSchedulerTotalQueryes;

            public long mSkipSchedulerCnt;

            public int mDeadlockCnt;

            public bool mIsEnable;

            public long mSchedulerSendedQueryes;
            public long mSchedulerHandledQueryes;

            public long mSetCallKernel;
            public long mSetSkipKernel;

            public long mWaitCallKernel;
            public long mWaitSkipKernel;

            public void OnSchedulerNewQuery() => Interlocked.Increment(ref mSchedulerSendedQueryes);
            public void OnSchedulerHandleQuery() => mSchedulerHandledQueryes++;
            public void AnalyzeScheduler(PoolTime pTick, PoolTime pNowTick)
            {
                PoolTime aResultTickDiff = pTick - pNowTick;

                Interlocked.Increment(ref mSchedulerTotalQueryes);

                if (aResultTickDiff < 0)
                    OnSchedulerDelay(-aResultTickDiff);
                else if (aResultTickDiff > 0)
                    OnSchedulerFaster(aResultTickDiff);
                else
                    Interlocked.Increment(ref mSchedulerPerfectHit);
            }
            public void OnDeadlock()
            {
                Interlocked.Increment(ref mDeadlockCnt);
            }
            public void OnWait(bool pSkipKernel)
            {
                if (pSkipKernel)
                    Interlocked.Increment(ref mWaitSkipKernel);
                else
                    Interlocked.Increment(ref mWaitCallKernel);
            }
            public void OnSet(bool pSkipKernel)
            {
                if (pSkipKernel)
                    Interlocked.Increment(ref mSetSkipKernel);
                else
                    Interlocked.Increment(ref mSetCallKernel);
            }
            public void OnSchedulerFaster(PoolTime pDelay)
            {
                Interlocked.Add(ref mSchedulerFasterTicks, pDelay);
                Interlocked.Increment(ref mSchedulerFasterTicksCnt);
            }

            public void OnSchedulerDelay(PoolTime pDelay)
            {
                Interlocked.Add(ref mSchedulerDelayTicks, pDelay);
                Interlocked.Increment(ref mSchedulerDelayTicksCnt);
            }
            public void OnSkipScheduler()
            {
                Interlocked.Increment(ref mSkipSchedulerCnt);
            }
        }

        public Action<Exception> mOnException;
        public Action<Dictionary<T, Dictionary<string, CWorkerMetrics.CMethodMetrics>>, string> mOnWorkerMetrics;

        private PropertyInfo threadOSIdProperty;
        private Action<string> mLogger;

        private ConcurrentStack<SPoolQuery> mQueryFactory;
        private SPoolQuery mFastQuery;

        private EventConcurrentQueue<SPoolQuery> mSchedulerQueryStack;
        // private AutoResetEvent mSchedulerWaitHandler;
        private CWorker mSchedulerWorker;

        private CWorker[] mWorkers;
        private CWorkerList[] mWorkersLists;
        private CWorkerList[] mWorkersHandlerIndexList;

        private bool mCompleted;

        public readonly CPoolMetrics mPoolMetrics;

        public const int TickRate = 64; // 64 ticks per second
        public bool ValidateThread { get; set; } = true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime GetMinutes(int pMin) => (Now() + (pMin * 60 * 1000));
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime GetSeconds(int pSeconds) => (Now() + pSeconds * 1000);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime GetMilliseconds(int pMillisecond)
        {
            return Now() + pMillisecond;

        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime FromDate(DateTime pDate)
        {
            int aMs = (int)(pDate - DateTime.Now).TotalMilliseconds;

            return GetMilliseconds(aMs);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime Now() => Environment.TickCount64;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static PoolTime NowTick() => NormalizeTick(Now());
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static PoolTime NormalizeTick(PoolTime pTick) => (PoolTime)((double)pTick / (MillisecondDelay()));
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static double MillisecondDelay() => (double)(1000.0f / TickRate);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static PoolTime NextTick()
        {
            PoolTime aRealNowTick = Now();
            int aTickms = (int)(MillisecondDelay() + 1);

            return aRealNowTick + (aTickms - (aRealNowTick % aTickms)) + 1;
        }

        public SyncWorkerPool
            (
                Action<string> pLogger,
                params CPoolWorkerConfig[] pHandlers
            )
        {
            threadOSIdProperty = Thread.CurrentThread.GetType().GetProperty("CurrentOSThreadId", BindingFlags.NonPublic | BindingFlags.Static);
            mLogger = pLogger;

            int aLastHandlerKey = -1;
            int aCountWorkers = 0;

            for (int i = 0; i < pHandlers.Length; ++i)
            {
                for (int j = 0; j < pHandlers[i].mHandlers.Length; ++j)
                {
                    int aThisHandler = BitOperations.Log2(Unsafe.As<T, uint>(ref pHandlers[i].mHandlers[j]));

                    if (aThisHandler > aLastHandlerKey)
                        aLastHandlerKey = aThisHandler;

                    aCountWorkers += pHandlers[i].mCountThreads;
                }
            }

            mWorkers = new CWorker[aCountWorkers + 1];
            mWorkersHandlerIndexList = new CWorkerList[aLastHandlerKey + 1];
            mWorkersLists = new CWorkerList[pHandlers.Length];

            int aWorkerIndex = 0;

            for (int i = 0; i < pHandlers.Length; ++i)
            {
                T[] aHandlers = pHandlers[i].mHandlers;
                int[] aHandlersKeys = new int[aHandlers.Length];

                for (int j = 0; j < aHandlers.Length; ++j)
                    aHandlersKeys[j] = Unsafe.As<T, int>(ref aHandlers[j]);

                CWorkerList aNewList = new CWorkerList(pHandlers[i].mCountThreads, aHandlersKeys);

                for (int j = 0; j < pHandlers[i].mCountThreads; ++j)
                {
                    EventConcurrentQueue<SPoolQuery> aNewQueue
                        = new EventConcurrentQueue<SPoolQuery>(new ConcurrentQueue<SPoolQuery>());

                    Thread aNewHandler = new Thread(worker) { IsBackground = true };

                    CWorker aNewWorker = new CWorker(aNewHandler)
                    {
                        mSkipPoolMetric = pHandlers[i].mSkipPoolMetric
                    };
                    aNewWorker.mConfigure = new CConfigureWorker()
                    {
                        mID = aWorkerIndex++,
                        mHandlers = aHandlersKeys,
                        mQueue = aNewQueue
                    };

                    aNewList.AddWorker(aNewWorker);

                    aNewHandler.Start(aNewWorker);
                    mWorkers[aNewWorker.mConfigure.mID] = aNewWorker;
                }

                for (int j = 0; j < aHandlersKeys.Length; ++j)
                    mWorkersHandlerIndexList[BitOperations.Log2((uint)aHandlersKeys[j])] = aNewList;

                mWorkersLists[i] = aNewList;

            }

            mPoolMetrics = new CPoolMetrics() { mIsEnable = false };

            mQueryFactory = new ConcurrentStack<SPoolQuery>();

            mSchedulerQueryStack = new EventConcurrentQueue<SPoolQuery>(new ConcurrentStack<SPoolQuery>());
            //  mSchedulerWaitHandler = new AutoResetEvent(false);
            Thread mSchedulerTh = new Thread(scheduler) { IsBackground = true, Priority = ThreadPriority.Highest };
            mSchedulerWorker = new CWorker(mSchedulerTh);
            mSchedulerTh.Start();
        }

        public void EnablePoolMetrics()
        {
            mPoolMetrics.mIsEnable = true;
        }

        public void PushQueryNonRet(T pHandler, Action pNotRetAct, PoolTime pProcTick = 0, bool pSync = false, int pLimitQuerySyncWait = -1, int pWorkerID = -1)
        {
            CheckDisposed();

            SPoolQuery aQuery = GetQuery(pWorkerID);
            bool aExceptionHappend = false;

            try
            {
                DelegateQuery(aQuery.MakeNonRetAct(pNotRetAct, pHandler, pProcTick, pSync));

                aQuery.Wait(pLimitQuerySyncWait);
            }
            catch
            {
                aExceptionHappend = true;
                throw;
            }
            finally
            {
                if (aExceptionHappend || pSync)
                    PushQueryToFactory(aQuery);
            }
        }
        public void PushQueryNonRet(T pHandler, Action<object> pNonRetAct, object pData, PoolTime pProcTick = 0, bool pSync = false, int pLimitQuerySyncWait = -1, int pWorkerID = -1)
        {
            CheckDisposed();

            SPoolQuery aQuery = GetQuery(pWorkerID);
            bool aExceptionHappend = false;

            try
            {
                DelegateQuery(aQuery.MakeNonRetParamAct(pNonRetAct, pData, pHandler, pProcTick, pSync));

                aQuery.Wait(pLimitQuerySyncWait);
            }
            catch
            {
                aExceptionHappend = true;
                throw;
            }
            finally
            {
                if (aExceptionHappend || pSync)
                    PushQueryToFactory(aQuery);
            }
        }
        public object PushQueryRet(T pHandler, Func<object> pRetFunc, PoolTime pProcTick = 0, bool pSync = false, int pLimitQuerySyncWait = -1, int pWorkerID = -1)
        {
            CheckDisposed();

            SPoolQuery aQuery = GetQuery(pWorkerID);
            bool aExceptionHappend = false;

            try
            {
                DelegateQuery(aQuery.MakeRetFunc(pRetFunc, pHandler, pProcTick, pSync));

                return aQuery.Wait(pLimitQuerySyncWait);
            }
            catch
            {
                aExceptionHappend = true;
                throw;
            }
            finally
            {
                if (aExceptionHappend || pSync)
                    PushQueryToFactory(aQuery);
            }
        }
        public object PushQueryRet(T pHandler, Func<object, object> pRetParamFunc, object pData, PoolTime pProcTick = 0, bool pSync = false, int pLimitQuerySyncWait = -1, int pWorkerID = -1)
        {
            CheckDisposed();

            SPoolQuery aQuery = GetQuery(pWorkerID);
            bool aExceptionHappend = false;

            try
            {
                DelegateQuery(aQuery.MakeRetParamFunc(pRetParamFunc, pData, pHandler, pProcTick, pSync));

                return aQuery.Wait(pLimitQuerySyncWait);
            }
            catch
            {
                aExceptionHappend = true;
                throw;
            }
            finally
            {
                if (aExceptionHappend || pSync)
                    PushQueryToFactory(aQuery);
            }
        }

        public void GetWorkerMetrics(T pHandler, int pLimitQuerySyncWait = -1)
        {
            CheckDisposed();

            if (mOnWorkerMetrics == null)
                throw new Exception($"SyncWorkerPool::GetWorkerMetrics: mOnWorkerMetrics callback is null");

            CWorkerList aNeedWorkers = GetWorkersAtHandler(pHandler);

            for (int i = 0; i < aNeedWorkers.mCountWorkers; ++i)
            {
                CWorker aNeedWorker = aNeedWorkers.mWorkers[i];

                if (!aNeedWorker.mMetricsIsEnabled)
                    throw new Exception($"SyncWorkerPool::GetWorkerMetrics: Worker{pHandler} metrics is disabled");

                SPoolQuery aQuery = GetQuery();
                bool aExceptionHappend = false;
                bool pSync = false;

                try
                {
                    DelegateQuery(aQuery._ServiceMakeCollectMetric(pHandler));

                    aQuery.Wait(pLimitQuerySyncWait);
                }
                catch
                {
                    aExceptionHappend = true;
                    throw;
                }
                finally
                {
                    if (aExceptionHappend || pSync)
                        PushQueryToFactory(aQuery);
                }
            }
        }
        public void SetCollectingMetrics(T pHandler)
        {
            CheckDisposed();
            CWorkerList aNeedWorker = GetWorkersAtHandler(pHandler);

            aNeedWorker.SetCollectingMetrics();
        }
        public void SetWorkerDeadlockTimeout(T pHandler, int pDeadlockTimeout)
        {
            CheckDisposed();
            CWorkerList aNeedWorkers = GetWorkersAtHandler(pHandler);

            aNeedWorkers.SetDeadlockTimeout(pDeadlockTimeout);
        }

        private CWorkerList GetWorkersAtHandler(T pHandler)
        {
            for (int i = 0; i < mWorkersLists.Length; ++i)
                for (int j = 0; j < mWorkersLists[i].mHandlers.Length; ++j)
                {
                    T aCurrentHandler = Unsafe.As<int, T>(ref mWorkersLists[i].mHandlers[j]);

                    if (aCurrentHandler.Equals(pHandler))
                    {
                        return mWorkersLists[i];
                    }
                }

            throw new Exception($"SyncWorkerPool::SetWorkerDeadlockTimeout({pHandler.ToString()}): worker not found.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DelegateQuery(SPoolQuery pQuery)
        {
            if (pQuery.CanExecuteNow())
            {
                if (mPoolMetrics.mIsEnable && pQuery.mTick > 0)
                    mPoolMetrics.OnSkipScheduler();

                PushToWorker(pQuery);
            }
            else
                ScheduleQuery(pQuery);
        }
        private CWorker GetWorkerAtID(int pWorkerID)
        {
            if (pWorkerID < 0 || pWorkerID >= mWorkers.Length)
                throw new Exception($"SyncWorkerPool::GetWorkerAtID({pWorkerID}): Wrong worker ID");

            CWorker aTargetWorker = mWorkers[pWorkerID];

            if (aTargetWorker != null)
                return aTargetWorker;
            else
                throw new Exception($"SyncWorkerPool::GetWorkerAtID({pWorkerID}): Worker is null");
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CWorker PushToWorker(SPoolQuery pQuery, bool pSkipValidateThread = false, bool pDoSet = true)
        {
            CheckDisposed();

            CWorker aTargetWorker;

            if (pQuery.mTargetWorkerID < 0)
            {
                aTargetWorker = mWorkersHandlerIndexList[ValidateHandler(pQuery.mHandler)].GetWorkerForExecuteQueue();
            }
            else
            {
                aTargetWorker = GetWorkerAtID(pQuery.mTargetWorkerID);
            }

            return AddQueryToWorker(pQuery, aTargetWorker, pSkipValidateThread, pDoSet: pDoSet);
        }

        private CWorker AddQueryToWorker(SPoolQuery pQuery, CWorker aTargetWorker, bool pSkipValidateThread = false, bool pDoSet = true)
        {
            if (!pSkipValidateThread && ValidateThread && pQuery.mSync)
            {
                int aCurrentTID = Thread.CurrentThread.ManagedThreadId;

                // Для вариантов когда запрос синхронный запрос вызван внутри воркера
                if (aTargetWorker.mTID == aCurrentTID)
                {
                    //  mLogger?.Invoke($"Prevent worker recursion query=[{pQuery.ToString()}]");
                    aTargetWorker.ProcessQuery(this, pQuery);
                    return null;
                }
            }

            aTargetWorker.mConfigure.mQueue.Add(pQuery);

            if (pDoSet)
            {
                if (aTargetWorker.mConfigure.mQueue.Set())
                { // call kernel
                    if (!aTargetWorker.mSkipPoolMetric && mPoolMetrics.mIsEnable) mPoolMetrics.OnSet(false);
                }
                else
                { // only CMPEX
                    if (!aTargetWorker.mSkipPoolMetric && mPoolMetrics.mIsEnable) mPoolMetrics.OnSet(true);
                }
            }

            return aTargetWorker;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ScheduleQuery(SPoolQuery pQuery)
        {
            CheckDisposed();

            mSchedulerQueryStack.Add(pQuery);

            bool aSetCallKernel = mSchedulerQueryStack.Set();

            if (mPoolMetrics.mIsEnable)
            {
                mPoolMetrics.OnSchedulerNewQuery();
                //   mPoolMetrics.OnSet(!aSetCallKernel);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            if (mCompleted)
                throw new Exception("Worker is disposed");
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ValidateHandler(T pHandler)
        {
            int aIndex = BitOperations.Log2(Unsafe.As<T, uint>(ref pHandler));

            if (aIndex < 0 || aIndex >= mWorkersHandlerIndexList.Length)
                throw new Exception($"SyncWorkerQuery::ValidateHandler({pHandler.ToString()}): invalid handler");

            if (mWorkersHandlerIndexList[aIndex] == null)
                throw new Exception($"SyncWorkerQuery::ValidateHandler({pHandler.ToString()}:{aIndex}): worker queue is null");

            return aIndex;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushQueryToFactory(SPoolQuery aFinishedQuery)
        {
            aFinishedQuery.Reset();

            if (mFastQuery == null && Interlocked.CompareExchange(ref mFastQuery, aFinishedQuery, null) == null)
                return;

            mQueryFactory.Push(aFinishedQuery);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private SPoolQuery GetQuery(int pWorkerID = -1)
        {
            SPoolQuery aFastestQuery = mFastQuery;

            if (aFastestQuery != null && Interlocked.CompareExchange(ref mFastQuery, null, aFastestQuery) == aFastestQuery)
                return aFastestQuery;

            if (mQueryFactory.TryPop(out SPoolQuery aUsedQuery))
            {
                aUsedQuery.mTargetWorkerID = pWorkerID;
                return aUsedQuery;
            }
            else
            {
                return new SPoolQuery() { mTargetWorkerID = pWorkerID };
            }
        }

        public int GetWorkerID(T pHandler)
        {
            CheckDisposed();
            return mWorkersHandlerIndexList[ValidateHandler(pHandler)].GetWorkerForExecuteQueue().mConfigure.mID;
        }

        private void WriteLog(string pData)
        {
            mLogger?.Invoke(pData);
        }

        private void checkDeadlock()
        {
            for (int i = 0; i < mWorkersLists.Length; ++i)
                mWorkersLists[i].CheckDeadlock(mLogger);
        }

        private void scheduler()
        {
            Thread.CurrentThread.Name = "SyncWorkerPool (Scheduler)";

            PriorityQueue<SPoolQuery, PoolTime> aLongQueryQueue = new PriorityQueue<SPoolQuery, PoolTime>();

            PoolTime aNowTick;
            PoolTime aRealTick;
            PoolTime aLastDeadlockCheck = 0;

            CTickRingBuffer<SPoolQuery> aShortQueryQueue = new CTickRingBuffer<SPoolQuery>(NowTick(), TickRate);
            HashSet<CWorker> aTriggeredWorkers = new HashSet<CWorker>();
            PoolTime aQueryExecuteTick = 0;
            int aLeftTicksToExecute = 0;

            mSchedulerQueryStack.BeginWait(4);

            do
            {
                try
                {
                    aRealTick = Now();
                    aNowTick = NormalizeTick(aRealTick);

                    if (aLastDeadlockCheck + (NormalizeTick(3000)) <= aNowTick)
                    {
                        checkDeadlock();
                        aLastDeadlockCheck = aNowTick;
                    }

                    aShortQueryQueue.ProcessItemsForTick(aNowTick, aRealTick, PushQuery);

                    while (mSchedulerQueryStack.TryTake(out SPoolQuery aNewQuery))
                    {
                        aQueryExecuteTick = NormalizeTick(aNewQuery.mTick);
                        aLeftTicksToExecute = (int)(aQueryExecuteTick - aNowTick);

                        if (aLeftTicksToExecute <= 0)
                        {
                            if (aLeftTicksToExecute == 0)
                            {
                                if (aRealTick < aNewQuery.mTick)
                                {
                                    aShortQueryQueue.Add(aNewQuery, aNowTick + 1);
                                    continue;
                                }
                            }

                            PushQuery(aRealTick, aNewQuery);
                        }
                        else if (aLeftTicksToExecute <= TickRate)
                        {
                            aShortQueryQueue.Add(aNewQuery, aQueryExecuteTick);
                        }
                        else
                        {
                            aLongQueryQueue.Enqueue(aNewQuery, aQueryExecuteTick);
                        }
                    }

                    while (aLongQueryQueue.TryPeek(out SPoolQuery aQuery, out PoolTime aTick))
                    {
                        aLeftTicksToExecute = (int)(aTick - aNowTick);

                        if (aLeftTicksToExecute <= 0)
                        {
                            PushQuery(aRealTick, aQuery);
                            aLongQueryQueue.Dequeue();
                        }
                        else if (aLeftTicksToExecute <= TickRate)
                        {
                            aShortQueryQueue.Add(aQuery, aTick);
                            aLongQueryQueue.Dequeue();
                        }
                        else
                            break;
                    }

                    foreach (var aWorker in aTriggeredWorkers)
                        aWorker.mConfigure.mQueue.Set();

                    aTriggeredWorkers.Clear();

                    if (mSchedulerQueryStack.EndWait(out bool aNewRound))
                    {
                        if (mPoolMetrics.mIsEnable)
                            mPoolMetrics.OnWait(false);

                        mSchedulerQueryStack.BeginWait(4);
                    }
                    else
                    {
                        if (aNewRound)
                            if (mPoolMetrics.mIsEnable)
                                mPoolMetrics.OnWait(true);
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        if (!mCompleted)
                            mOnException?.Invoke(e);
                    }
                    catch { }
                }
            } while (!mCompleted);

            void PushQuery(PoolTime aRealTick, SPoolQuery aQuery)
            {
                CWorker aThisWorker = PushToWorker(aQuery, pSkipValidateThread: true, pDoSet: false);

                if (aThisWorker != null)
                    aTriggeredWorkers.Add(aThisWorker);

                if (mPoolMetrics.mIsEnable)
                {
                    mPoolMetrics.AnalyzeScheduler(aQuery.mTick, aRealTick);
                    mPoolMetrics.OnSchedulerHandleQuery();
                }
            }
        }
        private void worker(object pObjectID)
        {
            CWorker aWorker = (CWorker)pObjectID;
            string tmpString = "";

            for (int i = 0; i < aWorker.mConfigure.mHandlers.Length; ++i)
            {
                //int tmpHandler = 1 << ();
                tmpString += (Unsafe.As<int, T>(ref aWorker.mConfigure.mHandlers[i])).ToString() + (i != aWorker.mConfigure.mHandlers.Length - 1 ? "|" : "");
            }
            aWorker.mPID = (int)((ulong)threadOSIdProperty.GetValue(aWorker.mThread));

            WriteLog($"Worker({aWorker.mConfigure.mID}) started. Handlers={tmpString} [TID={aWorker.mThread.ManagedThreadId}, PID={aWorker.mPID}]");

            aWorker.mThread.Name = $"SyncWorkerPool (Worker={aWorker.mConfigure.mID}) [{tmpString.ToString()}]";

            tmpString = null;
            SPoolQuery aNewQuery = null;

            aWorker.mConfigure.mQueue.BeginWait();

            do
            {
                try
                {
                    while (aWorker.mConfigure.mQueue.TryTake(out aNewQuery))
                    {
                        //aNewQuery = aWorker.mConfigure.mQueue.Take();

                        aWorker.mLastQueryStartTick = Now();
                        aWorker.mProcessingQuery = aNewQuery;

                        aWorker.ProcessQuery(this, aNewQuery);

                        aWorker.mProcessingQuery = null;
                    }

                    if (aWorker.mConfigure.mQueue.EndWait(out bool aNewRound))
                    {
                        if (!aWorker.mSkipPoolMetric && mPoolMetrics.mIsEnable)
                            mPoolMetrics.OnWait(false);

                        aWorker.mConfigure.mQueue.BeginWait();
                    }
                    else
                    {
                        if (aNewRound)
                            if (!aWorker.mSkipPoolMetric && mPoolMetrics.mIsEnable)
                                mPoolMetrics.OnWait(true);
                    }
                }
                catch (Exception e)
                {
                    try
                    {
                        if (e is ObjectDisposedException)
                        {
                            mOnException?.Invoke(e);
                        }
                        else if (mCompleted)
                            return;
                        else
                        {
                            mOnException?.Invoke(e);

                            if (aNewQuery != null)
                            {
                                if (aNewQuery.mSync)
                                    aNewQuery.Set();
                                else
                                    PushQueryToFactory(aNewQuery);
                            }
                        }
                    }
                    catch { }
                }
                finally
                {
                    aWorker.mLastQueryStartTick = 0;
                }
            } while (!mCompleted);
        }


        public void Close()
        {
            CheckDisposed();

            mCompleted = true;

            /*   for (int i = 0; i < mWorkersLists.Length; ++i)
                   for (int j = 0; j < mWorkersLists[i].mCountWorkers; ++j)
                       mWorkersLists[i].mWorkers[j].mConfigure.mQueue.CompleteAdding();
            */
            for (int i = 0; i < mWorkersLists.Length; ++i)
                for (int j = 0; j < mWorkersLists[i].mCountWorkers; ++j)
                    mWorkersLists[i].mWorkers[j].mThread.Join();

            //  mSchedulerWaitHandler.Set();
            mSchedulerWorker.mThread.Join();

            mQueryFactory.Clear();
            mQueryFactory = null;
        }

    }
}
