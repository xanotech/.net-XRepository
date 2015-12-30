using System;
using System.Web.Mvc;

namespace XRepository {
    public class HandleAjaxError : HandleErrorAttribute {
        public override void OnException(ExceptionContext context) {
            string message = "Unknown exception";
            string stack = "No stack trace available";
            
            // Believe it or not, getting an exception's message or its
            // ToString value can fail.  In those cases, just attempt
            // to set the stack to the full name of the exception type
            // and move on.  The exception itself is hosed so the type
            // name is about all you'll be able to get out of it (if that).
            try {
                message = context.Exception.Message;
                stack = context.Exception.ToString();
            } catch {
                try {
                    stack = context.Exception.GetType().FullName;
                } catch {
                } // end try-catch
            } // end try-catch

            context.Result = new JsonResult {
                Data = new { message = message, stack = stack },
                JsonRequestBehavior = JsonRequestBehavior.AllowGet
            };
            context.ExceptionHandled = true;
            context.RequestContext.HttpContext.Response.StatusCode = 500;
        }
    }
}
