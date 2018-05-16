defmodule Migrator.Drivers.Cassandra do

  def create_table_migrations do
    sql = "CREATE TABLE IF NOT EXISTS migrations (migrated_at timestamp PRIMARY KEY, migration ascii);"
    CQEx.Query.call(client, sql)
  end

  def fetch_applied_migrations do
    {:ok, migrations} = CQEx.Query.call(client, "SELECT * FROM migrations;")
    Enum.map migrations, fn(m) ->
      m[:migration]
    end
  end

  def add_applied_migration(date) do
    query = %CQEx.Query{
      statement: "INSERT INTO migrations(migrated_at, migration) VALUES(?, ?);",
      values:  %{migrated_at: :now, migration: date}
    }

    case CQEx.Query.call(client, query) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  def remove_applied_migration(migration) do
    query = %CQEx.Query{
      statement: "DELETE FROM migrations where migrated_at=?;",
      values:  %{migrated_at: migration[:migrated_at]}
    }

    case CQEx.Query.call(client, query) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  def last_migration_applied do
    case CQEx.Query.call!(client, "select * from migrations;") |> Enum.sort(&(&1[:migrated_at] >= &2[:migrated_at]))|>Enum.at(0) do
      nil -> nil
      migration -> migration
    end
  end

  def client do
    CQEx.Client.new!
  end

end
