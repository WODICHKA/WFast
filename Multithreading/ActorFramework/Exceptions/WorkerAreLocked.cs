using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class WorkerAreLocked : Exception
    {
        public WorkerAreLocked() : base("Worker are locked.") { }

    }
}
