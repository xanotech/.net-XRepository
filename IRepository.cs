using System.Collections.Generic;

namespace XRepository {
    public interface IRepository {
        long Count<T>() where T : new();
        long Count<T>(IEnumerable<Criterion> criteria) where T : new();
        long Count<T>(object criteria) where T : new();
        long Count<T>(params Criterion[] criteria) where T : new();

        Cursor<T> Find<T>() where T : new();
        Cursor<T> Find<T>(IEnumerable<Criterion> criteria) where T : new();
        Cursor<T> Find<T>(object criteria) where T : new();
        Cursor<T> Find<T>(params Criterion[] criteria) where T : new();

        T FindOne<T>() where T : new();
        T FindOne<T>(IEnumerable<Criterion> criteria) where T : new();
        T FindOne<T>(object criteria) where T : new();
        T FindOne<T>(params Criterion[] criteria) where T : new();

        bool IsReferenceAssignmentActive { get; set; }
        bool IsUsingLikeForEquals { get; set; }

        void Remove(object obj);

        void Save(object obj);
    } // end interface
} // end namespace