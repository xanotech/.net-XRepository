using System.Collections.Generic;

namespace Xanotech.Repository {
    public interface IRepository {
        void Delete(IIdentifiable obj);
        IEnumerable<T> Get<T>() where T : new();
        IEnumerable<T> Get<T>(IEnumerable<Criterion> criteria) where T : new();
        IEnumerable<T> Get<T>(IEnumerable<Criterion> criteria,
            IEnumerable<string> orderColumns) where T : new();
        T Get<T>(long id) where T : new();
        T Get<T>(long? id) where T : new();
        IEnumerable<T> Get<T>(object anonymousTypeCriteria) where T : new();
        IEnumerable<T> Get<T>(object anonymousTypeCriteria,
            IEnumerable<string> orderColumns) where T : new();
        IEnumerable<T> Get<T>(params Criterion[] criteria) where T : new();
        void Save(IIdentifiable obj);
    }
}
