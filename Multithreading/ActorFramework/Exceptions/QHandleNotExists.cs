using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class QHandleNotExists : Exception
    {
        public QHandleNotExists(int qHandle): base($"QueryHandle [{qHandle}] not exists in List") { }
    }
}
