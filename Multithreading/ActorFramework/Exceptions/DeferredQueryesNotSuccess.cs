using System;
using System.Collections.Generic;
using System.Text;

namespace WFast.ActorFramework.Exceptions
{
    public class DeferredQueryesNotSuccess : Exception
    {
        public DeferredQueryesNotSuccess(int n) : base($"DeferredQueryes not success [{n}]") { }

    }
}
