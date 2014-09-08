using System;
using System.Web.Mvc;

namespace XRepository {
    public class HandleAjaxError : HandleErrorAttribute {
        public override void OnException(ExceptionContext context) {
            var data = new {
                Message = context.Exception.Message,
                StackTrace = context.Exception.ToString()
            };
            context.Result = new JsonResult {
                Data = data,
                JsonRequestBehavior = JsonRequestBehavior.AllowGet
            };
            context.ExceptionHandled = true;
            context.RequestContext.HttpContext.Response.StatusCode = 500;
        }
    }
}
