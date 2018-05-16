defmodule Migrator do
  require Logger

  @driver Application.get_env(:migrator, :driver)
  @app_name  Application.get_env(:migrator, :app_name)

  def setup do
    @driver.create_table_migrations
  end

  def migrate do
    applied_migrations = @driver.fetch_applied_migrations
    migrations = list_available_migrations
    Enum.each list_available_migrations, fn(migration_name) ->
      [date, _name] = String.split migration_name, "-", parts: 2
      unless migration_applied? applied_migrations, date do
        Logger.info "Running migration #{migration_name}"
        run_migration date, migration_name
        Logger.info "Migration done #{migration_name}"
      end
    end
  end

  def rollback do
    case @driver.last_migration_applied do
      nil -> :ok
      last_migration ->
        migration_name = find_migration_file last_migration[:migration]
        Logger.info "Undoing migration #{migration_name}"
        undo_migration last_migration, migration_name
        Logger.info "Migration undone #{migration_name}"
        :ok
    end
  end

  def run_migration(date, file_name) do
    migration_file = "#{migrations_dir}/#{file_name}"
    {{:module, migrator_module, _,_}, []} = Code.eval_file migration_file
    case migrator_module.change do
      :error -> raise "Error running migration #{file_name}"
      _other -> :ok = @driver.add_applied_migration(date)
    end
  end

  def undo_migration(last_migration, file_name) do
    migration_file = "#{migrations_dir}/#{file_name}"
    {{:module, migrator_module, _,_}, []} = Code.eval_file migration_file
    if :erlang.function_exported(migrator_module, :undo, 0) do
      case migrator_module.undo do
        :error -> raise "Error undo migration #{file_name}"
        :ok -> :ok = @driver.remove_applied_migration(last_migration)
      end
    end
  end

  def migrations_dir do
    priv_dir = :code.priv_dir(@app_name)
    "#{priv_dir}/db/migrations"
  end

  def list_available_migrations do
    unless File.exists? migrations_dir do
      raise "Migrations dir #{migrations_dir} doesn't exists."
    end
    File.ls!(migrations_dir) |> Enum.filter fn(file) -> String.ends_with? file, ".exs" end
  end

  def migration_applied?(applied_migrations, date) do
    Enum.find applied_migrations, false, fn(m) ->
      m == date
    end
  end

  def find_migration_file(date) do
    File.ls!(migrations_dir) |> Enum.find fn(file) -> String.starts_with? file, "#{date}-" end
  end

end
