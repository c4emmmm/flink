package org.apache.flink.ds.iter.struct;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

import java.io.Serializable;
import java.lang.reflect.Field;

/**
 * @param <M>
 * @param <U>
 */
public class UnifiedModelInput<M, U> implements Serializable {
	public boolean isModel = false;
	public M model;

	public boolean isUpdate = false;
	public U update;

	public boolean isConvergeSignal = false;
	public ConvergeSignal convergeSignal;

	public static <M, U> UnifiedModelInput<M, U> wrapModel(M model) {
		UnifiedModelInput<M, U> in = new UnifiedModelInput<>();
		in.isModel = true;
		in.model = model;
		return in;
	}

	public static <M, U> UnifiedModelInput<M, U> wrapUpdate(U update) {
		UnifiedModelInput<M, U> in = new UnifiedModelInput<>();
		in.isUpdate = true;
		in.update = update;
		return in;
	}

	public static <M, U> UnifiedModelInput<M, U> wrapConvergeSignal(ConvergeSignal convergeSignal) {
		UnifiedModelInput<M, U> in = new UnifiedModelInput<>();
		in.isConvergeSignal = true;
		in.convergeSignal = convergeSignal;
		return in;
	}

	public static <M, U> TypeInformation<UnifiedModelInput<M, U>> returnType(
		TypeInformation<M> modelType, TypeInformation<U> updateType) {
		try {
			PojoTypeInfo typeInfo = (PojoTypeInfo) PojoTypeInfo.of(UnifiedModelInput.class);
			Field pojoTypeInfoFields = typeInfo.getClass().getDeclaredField("fields");
			pojoTypeInfoFields.setAccessible(true);
			PojoField[] fields = (PojoField[]) pojoTypeInfoFields.get(typeInfo);
			int modelIdx = -1;
			int updateIdx = -1;
			for (int i = 0; i < fields.length; i++) {
				PojoField f = fields[i];
				if (f.getField().getName().equals("model")) {
					modelIdx = i;
				} else if (f.getField().getName().equals("update")) {
					updateIdx = i;
				}
			}
			fields[modelIdx] = new PojoField(fields[modelIdx].getField(), modelType);
			fields[updateIdx] = new PojoField(fields[updateIdx].getField(), updateType);

			return typeInfo;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
