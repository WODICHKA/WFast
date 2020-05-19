using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class WorkerDisposed : Exception
    {
        public WorkerDisposed() : base("Worker disposed.") { }

    }
}
