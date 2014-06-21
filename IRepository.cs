using System.Collections.Generic;

namespace Xanotech.Repository {
    public interface IRepository {
        long Count<T>() where T : new();
        long Count<T>(IEnumerable<Criterion> criteria) where T : new();
        long Count<T>(long? id) where T : new();
        long Count<T>(object criteria) where T : new();
        long Count<T>(params Criterion[] criteria) where T : new();

        Cursor<T> Find<T>() where T : new();
        Cursor<T> Find<T>(IEnumerable<Criterion> criteria) where T : new();
        Cursor<T> Find<T>(long? id) where T : new();
        Cursor<T> Find<T>(object criteria) where T : new();
        Cursor<T> Find<T>(params Criterion[] criteria) where T : new();

        T FindOne<T>() where T : new();
        T FindOne<T>(IEnumerable<Criterion> criteria) where T : new();
        T FindOne<T>(long? id) where T : new();
        T FindOne<T>(object criteria) where T : new();
        T FindOne<T>(params Criterion[] criteria) where T : new();

        void Remove(object obj);

        void Save(object obj);
    }
}
