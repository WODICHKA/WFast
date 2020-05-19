using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using WFast.ActorFramework.Exceptions;

namespace WFast.ActorFramework
{
    public delegate object ParameterizedActorQueryRet(object _in);
    public delegate void ParameterizedActorQueryNoRet(object _in);
    public delegate DateTime DateTimeQuery();
    public delegate void ActorQuery();
    public delegate void OnException(int qHandler, string fInfo, int actorNo, Exception e);

    /*
     * Класс актеров без блокировок
     * при создании класса нужно указать максимальное кол-во актеров, и список хэндлеров
     * каждый хэндлер связывается с актером
     * StartActor() - возвращает actorNo номер актера к которому можно привязять хэндлер
     * AddQueryHandleToActor(actorNo, queryHandler) - хэндлер привязан к актеру 
     * все задачи под этим хэндлером будут идти к этому актеру или к пачке актеров (если один хэндлер привязан более чем к одному актеру) (Round-robin)
     * PushQuery(queryHandle, sync, () => { // some  }, -1) - эта задача будет отправлена одному из актеров с этим хэндлером
     * LockWorker() запрещает все изменения над классом, но ускоряет Push запросов т.к. не нужна синхронизация
     * Пример:
     * enum WorkerHandlers { DB = 1, Log = 2, Some...}
     * 
     * var worker = new SyncActorWorker(1, () => DateTime.Now, WorkerHandlers.DB, WorkerHandlers.Log);
     * 
     * int actorNo = worker.StartActor();
     * 
     * AddQueryHandleToActor(actorNo, WorkerHandlers.DB); Привязывает к актеру обработку работы с базой данных
     * AddQueryHandleToActor(actorNo, WorkerHandlers.Log);Привязывает к актеру обработку работы с логами
     * 
     * PushQuery(WorkerHandlers.Log, async, () => { File.WriteAllText("Hello world!"); }, -1)
     * PushQuery(WorkerHandlers.DB, sync, () => { postgre::query('drop all'); }, DateTime.Now.AddMinutes(1).Ticks); задача будет выполнена через минуту
     * 
     * worker.Destroy(true);
     * */


    /// <summary>
    /// Блокировки не нужны все операции синхронны
    /// </summary>
    public class SyncActorWorker
    {
        private class AQuery
        {
            public int QueryHandle { get; private set; }
            private AutoResetEvent _notify;

            private object _executor;
            private object _executorParam;
            private object _executorResult;

            public bool Sync { get; private set; }
            public long ExecuteTime { get; private set; }
            public string ExecutorString => _executor.ToString();

            private AQuery() { }
            public AQuery(int queryHandle, bool isSync, long executeTime, object executor, object executorParam = null)
            {
                this.QueryHandle = queryHandle;
                Sync = isSync;

                if (isSync)
                    _notify = new AutoResetEvent(false);

                _executor = executor;
                _executorParam = executorParam;
                ExecuteTime = executeTime;
                _executorResult = null;

                if (_executor == null)
                    throw new ArgumentNullException("executor", "AQuery()");
            }
            public object GetResult() => _executorResult;
            public void StartWait() => _notify?.WaitOne();
            private void MarkAsSuccess() => _notify?.Set();

            public void Process()
            {
                if (_executor is ActorQuery dQuery)
                    dQuery();
                else if (_executor is ParameterizedActorQueryNoRet noRetQuery)
                    noRetQuery(this._executorParam);
                else if (_executor is ParameterizedActorQueryRet RetQuery)
                    this._executorResult = RetQuery(this._executorParam);

                MarkAsSuccess();
            }
        }
        private class Actor
        {
            private const int ACTIVE_STATE = 1;
            private const int INACTIVE_STATE = 0;

            private AutoResetEvent _ARE;
            public int ActorNo { get; private set; }
            private Thread _actorThread;
            public SyncActorWorker Worker;
            private ConcurrentQueue<AQuery> _aQueue;
            private volatile int _actorState;
            private Actor() { }
            public Actor(ThreadPriority thPriority = ThreadPriority.Normal)
            {
                _actorThread = new Thread(new ParameterizedThreadStart(Actor.worker));
                _actorThread.IsBackground = true;
                _actorThread.Priority = thPriority;
                _ARE = new AutoResetEvent(false);

                _aQueue = new ConcurrentQueue<AQuery>();
            }
            public void Destroy(bool withJoin = true)
            {
                if (_actorState != ACTIVE_STATE)
                    return;

                _actorState = INACTIVE_STATE;

                if (withJoin)
                {
                    _ARE.Set();
                    _actorThread.Join();
                }
            }
            public bool IsActive() => _actorState == ACTIVE_STATE;
            public void StartWorker(int actorNo)
            {
                if (_actorState != INACTIVE_STATE)
                    throw new ActorAlreadyWork(this.ActorNo);

                this.ActorNo = actorNo;
                _actorState = ACTIVE_STATE;
                _actorThread.Start(this);
            }
            public void PushQuery(AQuery newQuery)
            {
                _aQueue.Enqueue(newQuery);

                _ARE.Set();
            }
            private static void worker(object _actorObj)
            {
                Actor thisActor = (Actor)_actorObj;

                try
                {
                    while (thisActor.IsActive())
                    {
                        thisActor._ARE.WaitOne();

                        while (thisActor._aQueue.TryDequeue(out AQuery query))
                        {
                            try
                            {
                                query.Process();
                            }
                            catch (Exception e)
                            {
                                if (thisActor.Worker != null && thisActor.Worker.OnUserException != null)
                                    thisActor.Worker.OnUserException(query.QueryHandle, query.ExecutorString, thisActor.ActorNo, e);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    if (thisActor.Worker != null && thisActor.Worker.OnActorException != null)
                        thisActor.Worker.OnActorException(-1, "", thisActor.ActorNo, e);

                    if (thisActor.Worker != null)
                        thisActor.Worker.RemoveActor(thisActor.ActorNo, true);
                    return;
                }
            }

            public override int GetHashCode()
            { return base.GetHashCode(); }

            public override bool Equals(object obj)
            {
                if (obj == null || !(obj is Actor))
                    return false;

                return (obj as Actor).ActorNo == this.ActorNo;
            }
        }

        private struct _maintainQueryStruct
        {
            public int actorNo;
            public int queryHandle;
        }

        private int _actorLastNo;

        private List<Actor> _actorsList;
        private Dictionary<int, List<Actor>> _actorsQHandlers;
        private Dictionary<int, int> _actorsRoundValue;
        public int ActiveActors { get; private set; }
        /// <summary>
        /// Вызывается когда в одном из актеров произошла ошибка в задаче
        /// </summary>
        public OnException OnUserException;
        /// <summary>
        /// Критическая ситуация после вызова уничтожается актер породивший или уничтожается весь воркер (если actorNo = -1)
        /// </summary>
        public OnException OnActorException;

        private int _maxActors;
        private DateTimeQuery getTime;

        private List<AQuery> deferredQueryes;
        private Thread serviceThread;
        private ConcurrentQueue<AQuery> serviceQueue;
        private AutoResetEvent serviceEventNotify;
        public bool IsWorking { get; private set; }
        private bool workerLocked;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="maxActors">максимальное кол-во актеров</param>
        /// <param name="_getTime">необходимо для работы отложенных запросов например: [ () => DateTime.Now ]</param>
        /// <param name="qHandlers">список всех возможных хэндлеров</param>
        public SyncActorWorker(int maxActors, DateTimeQuery _getTime, params int[] qHandlers)
        {
            if (maxActors <= 0)
                throw new ArgumentOutOfRangeException("maxActors");
            if (_getTime == null)
                throw new ArgumentNullException("_getTime");

            getTime = _getTime;

            _actorLastNo = 1;
            _maxActors = maxActors;

            _actorsList = new List<Actor>(_maxActors);
            _actorsQHandlers = new Dictionary<int, List<Actor>>(qHandlers.Length);
            _actorsRoundValue = new Dictionary<int, int>();
            deferredQueryes = new List<AQuery>();

            foreach (var thisHandler in qHandlers)
            {
                if (_actorsQHandlers.ContainsKey(thisHandler))
                    throw new ArgumentException("qHandlers");
                if (thisHandler < 0)
                    throw new ArgumentException("qHandlers < 0");

                _actorsQHandlers.Add(thisHandler, new List<Actor>(maxActors));
                _actorsRoundValue.Add(thisHandler, 0);
            }

            workerLocked = false;
            serviceEventNotify = new AutoResetEvent(false);
            serviceQueue = new ConcurrentQueue<AQuery>();

            IsWorking = true;
            serviceThread = new Thread(serviceWorker);
            serviceThread.IsBackground = true;
            serviceThread.Start();
        }
        /// <summary>
        /// Создает и запускает актера возвращает номер актера
        /// Нельзя использовать если воркер "залочен"
        /// </summary>
        /// <param name="priority"></param>
        /// <returns></returns>
        public int StartActor(ThreadPriority priority = ThreadPriority.Normal)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            if (workerLocked)
                throw new WorkerAreLocked();

            if (ActiveActors == _maxActors)
                throw new TooMuchActors(ActiveActors, _maxActors);

            int newActorNo
                = (int)handleQuery(new AQuery(-1, true, -1, new ParameterizedActorQueryRet(makeActor), priority), true);

            return newActorNo;
        }
        /// <summary>
        /// Удаляет все связи с актером останавливает и дожидается завершения работы
        /// Нельзя использовать если воркер "залочен"
        /// </summary>
        /// <param name="actorNo"></param>
        /// <param name="dontWait">не ждет завершения работы</param>
        public void RemoveActor(int actorNo, bool dontWait = false)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            if (workerLocked)
                throw new WorkerAreLocked();

            if (ActiveActors == 0)
                throw new ActorNotExists(actorNo);

            object result = handleQuery(new AQuery(-1, !dontWait, -1, new ParameterizedActorQueryRet(removeActor), actorNo), true);

            if (result != null)
                throw result as Exception;

            return;
        }
        /// <summary>
        /// Освобождает все ресурсы дожидается завершения всех актеров и главного треда
        /// </summary>
        /// <param name="skipDeferredQueryes">завершает даже если есть не завершенные задачи</param>
        public void Destroy(bool skipDeferredQueryes)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            object result
                = handleQuery(new AQuery(-1, true, -1, new ParameterizedActorQueryRet(_destroyWorker), skipDeferredQueryes), true);

            if (result != null)
                throw result as Exception;
            else
                serviceThread.Join();
        }

        private object _destroyWorker(object skipObj)
        {
            bool skipDefQueryes = (bool)skipObj;

            if (deferredQueryes.Count > 0 && !skipDefQueryes)
                return new DeferredQueryesNotSuccess(deferredQueryes.Count);

            for (int i = 0; i < ActiveActors; ++i)
            {
                if (i < 0)
                    i = 0;

                Actor thisActor = _actorsList[i];

                removeActor(thisActor.ActorNo);
                --i;
            }

            IsWorking = false;
            serviceEventNotify.Set();
            serviceQueue = null;
            serviceEventNotify = null;
            _actorsList = null;
            _actorsQHandlers = null;
            _actorsRoundValue = null;
            deferredQueryes = null;

            return null;
        }
        private object removeActor(object noObj)
        {
            int actorNo = (int)noObj;

            for (int i = 0; i < ActiveActors; ++i)
            {
                Actor thisActor = _actorsList[i];

                if (thisActor.ActorNo == actorNo)
                {
                    foreach (var thisQActors in _actorsQHandlers)
                        thisQActors.Value.Remove(thisActor);

                    thisActor.Destroy();
                    _actorsList.RemoveAt(i);
                    --ActiveActors;

                    return null;
                }
            }

            return new ActorNotExists(actorNo);
        }
        private object makeActor(object thPriorityObj)
        {
            ThreadPriority priority = (ThreadPriority)thPriorityObj;

            Actor newActor = new Actor(priority);
            newActor.Worker = this;

            int newActorNo = _actorLastNo++;

            ++ActiveActors;
            newActor.StartWorker(newActorNo);

            _actorsList.Add(newActor);
            return newActorNo;
        }

        /// <summary>
        /// Пушит задачу
        /// </summary>
        /// <param name="queryHandle">номер хэндлера</param>
        /// <param name="query">задача без параметра и без возвращаемого значения</param>
        /// <param name="isSync">синронно/асинронно</param>
        /// <param name="executeTime">время когда нужно выполнить задачу (в тиках) DateTime.Now.AddSecond(60).Ticks</param>
        public void PushQuery(int queryHandle, ActorQuery query, bool isSync = false, long executeTime = -1)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            delegateQuery(makeQuery(queryHandle, query, isSync, executeTime, null));
        }
        /// <summary>
        /// Пушит задачу
        /// </summary>
        /// <param name="queryHandle">номер хэндлера</param>
        /// <param name="query">задача с параметром без возвращаемого значения</param>
        /// <param name="queryArgument">аргумент(object)</param>
        /// <param name="isSync">синронно/асинронно</param>
        /// <param name="executeTime">время когда нужно выполнить задачу (в тиках) DateTime.Now.AddSecond(60).Ticks</param>
        public void PushQuery(int queryHandle, ParameterizedActorQueryNoRet query, object queryArgument, bool isSync = false, long executeTime = -1)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            delegateQuery(makeQuery(queryHandle, query, isSync, executeTime, queryArgument));
        }
        /// <summary>
        /// Пушит задачу синронно и возвращает результат
        /// </summary>
        /// <param name="queryHandle">номер хэндлера</param>
        /// <param name="query">задача с параметром и с возвращаемым значением</param>
        /// <param name="queryArgument">аргумент(object)</param>
        /// <param name="executeTime">время когда нужно выполнить задачу (в тиках) DateTime.Now.AddSecond(60).Ticks</param>
        /// <returns></returns>
        public object PushQuerySync(int queryHandle, ParameterizedActorQueryRet query, object queryArgument, long executeTime = -1)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            return delegateQuery(this.makeQuery(queryHandle, query, true, executeTime, queryArgument));
        }

        /// <summary>
        /// Блокирует воркер запрещает все изменения с актерами.
        /// Значительно ускоряет Push()
        /// </summary>
        public void LockWorker()
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            workerLocked = true;
        }
        /// <summary>
        /// Удаляет "хэндлер" с актера
        /// </summary>
        /// <param name="actorNo">номер актера</param>
        /// <param name="queryHandle">хэндлер</param>
        public void RemoveQueryHandleFromActor(int actorNo, int queryHandle)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            if (workerLocked)
                throw new WorkerAreLocked();

            if (queryHandle < 0)
                throw new ArgumentException("queryHandle < 0");

            AQuery removeQueryQ = new AQuery(-1, true, -1,
                new ParameterizedActorQueryRet(
                    (qObj) =>
                    {
                        _maintainQueryStruct qInfo = (_maintainQueryStruct)qObj;

                        Actor thisActor = getActor(qInfo.actorNo);

                        if (thisActor == null)
                            return new ActorNotExists(qInfo.actorNo);

                        if (_actorsQHandlers.TryGetValue(qInfo.queryHandle, out List<Actor> currentActorList))
                        {
                            if (!currentActorList.Remove(thisActor))
                                return new QHandleNotExists(qInfo.queryHandle);
                        }
                        else
                            return new QHandleNotExists(qInfo.queryHandle);

                        return null;
                    }),
                new _maintainQueryStruct() { actorNo = actorNo, queryHandle = queryHandle });

            object result = handleQuery(removeQueryQ, true);

            if (result != null)
                throw result as Exception;
        }
        /// <summary>
        /// Добавляет "хэндлер" актеру
        /// </summary>
        /// <param name="actorNo">номер актера</param>
        /// <param name="queryHandle">хэндлер</param>
        public void AddQueryHandleToActor(int actorNo, int queryHandle)
        {
            if (!IsWorking)
                throw new WorkerDisposed();

            if (workerLocked)
                throw new WorkerAreLocked();

            if (queryHandle < 0)
                throw new ArgumentException("queryHandle < 0");

            AQuery addQueryQ = new AQuery(-1, true, -1,
                new ParameterizedActorQueryRet(
                    (qObj) =>
                    {
                        _maintainQueryStruct qInfo = (_maintainQueryStruct)qObj;

                        Actor thisActor = getActor(qInfo.actorNo);

                        if (thisActor == null)
                            return new ActorNotExists(qInfo.actorNo);

                        if (_actorsQHandlers.TryGetValue(qInfo.queryHandle, out List<Actor> currentActorList))
                        {
                            if (currentActorList.Contains(thisActor))
                                return new ActorAlreadyAddedInThisQGroup(qInfo.actorNo);

                            currentActorList.Add(thisActor);
                        }
                        else
                            return new QHandleNotExists(qInfo.queryHandle);

                        return null;
                    }),
                new _maintainQueryStruct() { actorNo = actorNo, queryHandle = queryHandle });

            object result = handleQuery(addQueryQ, true);

            if (result != null)
                throw result as Exception;
        }

        private void serviceWorker()
        {
            try
            {
                int countDeferredQueryes = 0;

                while (IsWorking)
                {
                    serviceEventNotify.WaitOne(100);

                    while (serviceQueue.TryDequeue(out AQuery q))
                    {
                        q.Process();

                        if (!IsWorking)
                            return;
                    }

                    countDeferredQueryes = deferredQueryes.Count;

                    long nowMoment = getTime().Ticks;

                    for (int i = 0; i < countDeferredQueryes; ++i)
                    {
                        AQuery currentQuery = deferredQueryes[i];

                        if (nowMoment >= currentQuery.ExecuteTime)
                        {
                            deferredQueryes.RemoveAt(i);

                            i--;
                            countDeferredQueryes--;

                            if (i < 0)
                                i = 0;

                            handleQuery(currentQuery);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (OnActorException != null)
                    OnActorException(-1, "serviceWorker", -1, e);

                _destroyWorker(true);
            }
        }

        private Actor getActor(int actorNo)
        {
            int countActors = _actorsList.Count;

            for (int i = 0; i < countActors; ++i)
            {
                Actor thisActor = _actorsList[i];

                if (thisActor == null)
                    continue;

                if (thisActor.ActorNo == actorNo)
                    return thisActor;
            }

            return null;
        }
        private AQuery makeQuery(int queryHandle, object executor, bool isSync, long executionTime, object executorArgument)
        {
            if (executionTime == -1)
                executionTime = getTime().Ticks - 1;

            if (queryHandle < 0)
                throw new ArgumentException("queryHandle < 0");

            return new AQuery(queryHandle, isSync, executionTime, executor, executorArgument);
        }
        private object delegateQuery(AQuery query)
        {
            long nowMoment = getTime().Ticks;

            if (nowMoment >= query.ExecuteTime)
            {
                return handleQuery(query);
            }
            else
            {
                if (!query.Sync)
                {
                    handleQuery(new AQuery(-1, false, -1, new ParameterizedActorQueryNoRet((q) =>
                    {
                        AQuery defQuery = (AQuery)q;
                        deferredQueryes.Add(defQuery);
                    }), query), true);

                    return null;
                }
                else
                {
                    do
                    {
                        int secondsNeedToWait = (int)Math.Ceiling((decimal)(new DateTime(query.ExecuteTime) - getTime()).TotalSeconds);
                        secondsNeedToWait *= 1000;
                        if (secondsNeedToWait <= 0)
                            secondsNeedToWait = 1;

                        Thread.Sleep(secondsNeedToWait);
                        nowMoment = getTime().Ticks;
                    } while (nowMoment < query.ExecuteTime);

                    return handleQuery(query);
                }
            }
        }
        private object handleQuery(AQuery query, bool _workerQuery = false)
        {
            if (_workerQuery)
                pushQueryToServiceWorkerNoexcept(query);
            else
            {
                if (workerLocked)
                    pushAQueryToActors(query);
                else
                {
                    object result =
                        handleQuery(new AQuery(-1, true, -1, new ParameterizedActorQueryRet(pushQueryToActorsNoexcept), query), true);

                    if (result != null)
                        throw result as Exception;

                }
            }

            if (query.Sync)
            {
                query.StartWait();
                return query.GetResult();
            }
            else
            {
                return null;
            }
        }

        private void pushQueryToServiceWorkerNoexcept(AQuery thisQuery)
        {
            serviceQueue.Enqueue(thisQuery);
            serviceEventNotify.Set();
        }
        private object pushQueryToActorsNoexcept(object queryObj)
        {
            AQuery thisQuery = (AQuery)queryObj;

            if (_actorsQHandlers.TryGetValue(thisQuery.QueryHandle, out List<Actor> currentActorsList))
            {
                int countActors = currentActorsList.Count;

                Actor deletegatedActor = null;

                if (countActors == 1)
                    deletegatedActor = currentActorsList[0];
                else if (countActors == 0)
                    return new ActorNotExists(-1);
                else
                {
                    int actorArrayNo = _actorsRoundValue[thisQuery.QueryHandle];

                    if (countActors <= actorArrayNo)
                        actorArrayNo = 0;

                    deletegatedActor = currentActorsList[actorArrayNo];

                    _actorsRoundValue[thisQuery.QueryHandle] = ++actorArrayNo;
                }
                if (deletegatedActor == null)
                    return new NullReferenceException("deletegatedActor");

                deletegatedActor.PushQuery(thisQuery);
                return null;
            }
            else
                return new QHandleNotExists(thisQuery.QueryHandle);

        }
        private void pushAQueryToActors(AQuery thisQuery)
        {
            object rslt = null;

            if (!this.workerLocked)
                rslt = pushQueryToActorsNoexcept(thisQuery);
            else
                rslt = handleQuery(new AQuery(-1, true, -1, new ParameterizedActorQueryRet(pushQueryToActorsNoexcept), thisQuery), true);

            if (rslt != null)
                throw rslt as Exception;
        }
    }
}
